package godbus

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/godbus/dbus/v5"
	"go.uber.org/zap"
)

type Member string

const (
	SYSTEM_NAME_ACQUIRED_MEMBER Member    = "NameAcquired"
	SYSTEM_DBUS_INTERFACE       Interface = "org.freedesktop.DBus"
	SYSTEM_DBUS_PATH            Path      = "/org/freedesktop/DBus"
)

func (e Member) String() string {
	return string(e)
}

func (e Member) ACK() Member {
	return Member(fmt.Sprintf("%s_ack", e))
}

func (e Member) IsACK() bool {
	return strings.HasSuffix(string(e), "_ack")
}

type Path dbus.ObjectPath

func (p Path) String() string {
	return string(p)
}

type Interface string

func (i Interface) String() string {
	return string(i)
}

type DBusPayload struct {
	Interface Interface
	Path      Path
	Member    Member
	Body      []any
}

func (p DBusPayload) Name() string {
	return fmt.Sprintf("%s.%s", p.Interface, p.Member)
}

func (p DBusPayload) IsSystemNameAcquired() bool {
	return p.Interface == SYSTEM_DBUS_INTERFACE &&
		p.Path == SYSTEM_DBUS_PATH &&
		p.Member == SYSTEM_NAME_ACQUIRED_MEMBER
}

func (p DBusPayload) IsSystemBus() bool {
	return strings.HasPrefix(p.Interface.String(), "org.freedesktop")
}

type BusSignalHandler func(
	ctx context.Context,
	payload DBusPayload) ([]any, error)

type DBusClient struct {
	sync.Mutex

	ctx               context.Context
	conn              *dbus.Conn
	sigChan           chan *dbus.Signal
	doneChan          chan struct{}
	logger            *zap.Logger
	busSignalHandlers []BusSignalHandler
	serviceID         *string
	serviceName       string
	matchOptions      []dbus.MatchOption
}

func NewDBusClient(ctx context.Context, logger *zap.Logger, serviceName string, matchOptions ...dbus.MatchOption) *DBusClient {
	return &DBusClient{
		ctx:               ctx,
		sigChan:           make(chan *dbus.Signal, 10),
		doneChan:          make(chan struct{}),
		logger:            logger,
		busSignalHandlers: []BusSignalHandler{},
		matchOptions:      matchOptions,
		serviceName:       serviceName,
	}
}

func (c *DBusClient) Start() error {
	c.logger.Info("Starting DBusClient")
	conn, err := dbus.SessionBus()
	if err != nil {
		return err
	}
	c.conn = conn
	conn.Signal(c.sigChan)

	// Request service name
	rel, err := conn.RequestName(c.serviceName, dbus.NameFlagDoNotQueue)
	if err != nil {
		return err
	}
	if rel != dbus.RequestNameReplyPrimaryOwner {
		return fmt.Errorf("failed to request name: %d", rel)
	}

	// Get service ID acquired
	names := conn.Names()
	if len(names) == 0 {
		return fmt.Errorf("no names on the bus")
	}
	c.Lock()
	c.serviceID = &names[0]
	c.logger.Debug("Service ID acquired", zap.String("serviceID", *c.serviceID))
	c.Unlock()

	// Add match options
	err = conn.AddMatchSignalContext(c.ctx, c.matchOptions...)
	if err != nil {
		return err
	}

	c.background()

	return nil
}

func (c *DBusClient) Export(v any, path Path, iface Interface) error {
	c.Lock()
	defer c.Unlock()

	if c.conn == nil {
		return fmt.Errorf("DBusClient not started")
	}

	return c.conn.Export(v, dbus.ObjectPath(path), iface.String())
}

func (c *DBusClient) background() {
	go func() {
		c.logger.Info("DBusClient background goroutine started")
		for {
			select {
			case <-c.ctx.Done():
				c.logger.Debug("Context cancelled, stopping DBusClient")
				err := c.Stop()
				if err != nil {
					c.logger.Error("Failed to stop DBusClient", zap.Error(err))
				}
				return
			case <-c.doneChan:
				c.logger.Debug("DBusClient stopped")
				return
			case sig := <-c.sigChan:
				if sig == nil {
					c.logger.Warn("Received nil signal")
					continue
				}

				c.logger.Debug("Received DBus signal", zap.String("sender", sig.Sender), zap.String("name", sig.Name), zap.String("path", string(sig.Path)))
				if err := c.handleSignalRecv(sig); err != nil {
					c.logger.Error("Failed to handle signal", zap.Error(err))
				}
			}
		}
	}()
}

func (c *DBusClient) OnBusSignal(f BusSignalHandler) {
	c.Lock()
	defer c.Unlock()
	c.logger.Debug("Adding bus signal handler", zap.String("handler pointer", fmt.Sprintf("%p", f)))
	c.busSignalHandlers = append(c.busSignalHandlers, f)
}

func (c *DBusClient) RemoveBusSignal(f BusSignalHandler) {
	c.Lock()
	defer c.Unlock()

	c.logger.Debug("Removing bus signal handler", zap.String("handler pointer", fmt.Sprintf("%p", &f)))
	for i, handler := range c.busSignalHandlers {
		if fmt.Sprintf("%p", handler) == fmt.Sprintf("%p", &f) {
			c.busSignalHandlers = append(c.busSignalHandlers[:i], c.busSignalHandlers[i+1:]...)
			c.logger.Debug("Removed bus signal handler", zap.String("handler pointer", fmt.Sprintf("%p", &f)))
			break
		}
	}
}

// handleSignalRecv handles a received signal that's not an ACK
func (c *DBusClient) handleSignalRecv(sig *dbus.Signal) error {
	c.Lock()
	if c.serviceID != nil && *c.serviceID == sig.Sender {
		c.Unlock()
		c.logger.Debug("Skip self-generated signal", zap.String("name", sig.Name), zap.String("path", string(sig.Path)))
		return nil
	}
	c.Unlock()

	i := strings.LastIndex(sig.Name, ".")
	if i == -1 {
		return fmt.Errorf("invalid signal name: %s", sig.Name)
	}
	iface := Interface(sig.Name[:i])
	member := Member(sig.Name[i+1:])
	path := Path(sig.Path)

	payload := DBusPayload{
		Interface: iface,
		Path:      path,
		Member:    member,
		Body:      sig.Body,
	}

	// Skip system bus signals
	if payload.IsSystemBus() {
		if payload.IsSystemNameAcquired() {
			if len(sig.Body) == 0 {
				return fmt.Errorf("system name acquired signal has no body")
			}
			senderID, ok := sig.Body[0].(string)
			if !ok {
				return fmt.Errorf("system name acquired signal body doesn't contain a sender ID string")
			}
			c.Lock()
			c.serviceID = &senderID
			c.Unlock()
		}

		return nil
	}

	// Ensure senderID is set
	c.Lock()
	if c.serviceID == nil {
		c.Unlock()
		return fmt.Errorf("senderID is not set")
	}
	c.Unlock()

	// Send ACK
	if !member.IsACK() {
		err := c.SendACK(payload)
		if err != nil {
			c.logger.Warn("Failed to send ACK", zap.String("name", payload.Name()), zap.String("path", payload.Path.String()), zap.Error(err))
		}
	}

	for _, handler := range c.busSignalHandlers {
		p := payload
		h := handler

		// Run the handler in a separate goroutine to avoid blocking the main thread
		go func(ctx context.Context, payload DBusPayload, handler BusSignalHandler) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.doneChan:
				return fmt.Errorf("DBusClient stopped")
			default:
				// Handle signal
				_, err := handler(ctx, payload)
				if err != nil {
					c.logger.Warn("Failed to handle signal", zap.String("interface", iface.String()), zap.String("path", path.String()), zap.String("member", member.String()), zap.Error(err))
					return err
				}
				return nil
			}
		}(c.ctx, p, h)
	}

	return nil
}

func (c *DBusClient) SendACK(payload DBusPayload) error {
	return c.Send(DBusPayload{
		Interface: payload.Interface,
		Path:      payload.Path,
		Member:    payload.Member.ACK(),
		Body:      nil,
	})
}

// RetryableSend retries sending a signal until an ACK is received
// This function will be blocked until an ACK is received or the context is cancelled or the backoff timer expires
// So it should be called in a separate goroutine unless you want to block the main thread
func (c *DBusClient) RetryableSend(ctx context.Context, payload DBusPayload) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 5 * time.Second
	bo.MaxElapsedTime = 30 * time.Second

	// Create a channel to receive ACK
	ackChan := make(chan struct{})
	var once sync.Once

	// Create a temporary handler to listen for ACK
	var handler BusSignalHandler
	handler = func(ctx context.Context, p DBusPayload) ([]any, error) {
		// Check if this is the ACK for our signal
		if p.Member == payload.Member.ACK() {
			once.Do(func() {
				close(ackChan)
			})

			// Remove this temporary handler
			c.RemoveBusSignal(handler)
			return nil, nil
		}
		return nil, nil
	}

	// Add the temporary handler
	c.OnBusSignal(handler)
	defer c.RemoveBusSignal(handler)

	// Retry ops
	attempts := 0
	ops := func() error {
		attempts++

		// Send the signal
		if err := c.Send(payload); err != nil {
			c.logger.Error("Failed to send signal", zap.Error(err))
			return err
		}

		// Wait for ACK with timeout
		select {
		case <-ackChan:
			return nil // ACK received, success
		case <-ctx.Done():
			return backoff.Permanent(ctx.Err()) // Context cancelled
		case <-time.After(bo.NextBackOff()):
			return fmt.Errorf("timeout waiting for ACK") // Timeout, will retry
		}
	}

	return backoff.Retry(ops, bo)
}

func (c *DBusClient) Send(payload DBusPayload) error {
	c.Lock()
	defer c.Unlock()

	c.logger.Debug("Sending signal", zap.String("name", payload.Name()), zap.String("path", payload.Path.String()))
	return c.conn.Emit(dbus.ObjectPath(payload.Path), payload.Name(), payload.Body...)
}

func (c *DBusClient) callMethod(ctx context.Context, serviceName string, path Path, iface Interface, member Member, args ...any) (*dbus.Call, error) {
	c.Lock()
	defer c.Unlock()

	if c.conn == nil {
		return nil, fmt.Errorf("DBusClient not started")
	}

	obj := c.conn.Object(serviceName, dbus.ObjectPath(path))
	call := obj.CallWithContext(ctx, fmt.Sprintf("%s.%s", iface, member), 0, args...)
	if call.Err != nil {
		return nil, call.Err
	}

	return call, nil
}

// Call sends a method call and returns the response body.
func (c *DBusClient) Call(ctx context.Context, serviceName string, path Path, iface Interface, member Member, args ...any) ([]any, error) {
	call, err := c.callMethod(ctx, serviceName, path, iface, member, args...)
	if err != nil {
		return nil, err
	}
	return call.Body, nil
}

// Query sends a method call and stores the response in the provided destination. (single value)
func (c *DBusClient) Query(ctx context.Context, dest interface{}, serviceName string, path Path, iface Interface, member Member, args ...any) error {
	call, err := c.callMethod(ctx, serviceName, path, iface, member, args...)
	if err != nil {
		return err
	}
	return call.Store(dest)
}

// Scan sends a method call and stores the response in the provided destination slice. (multiple values)
func (c *DBusClient) Scan(ctx context.Context, dest []interface{}, serviceName string, path Path, iface Interface, member Member, args ...any) error {
	call, err := c.callMethod(ctx, serviceName, path, iface, member, args...)
	if err != nil {
		return err
	}
	return call.Store(dest...)
}

func (c *DBusClient) Stop() error {
	c.Lock()
	defer c.Unlock()

	c.logger.Info("Stopping DBusClient")

	select {
	case <-c.doneChan:
		// Already closed
	default:
		close(c.doneChan)
	}

	if c.conn != nil {
		err := c.conn.Close()
		if err != nil {
			return err
		}
		c.conn = nil
		c.logger.Info("DBusClient connection closed")
	}

	return nil
}
