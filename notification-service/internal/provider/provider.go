package provider

// EmailSender is the adapter interface — business logic depends only on this.
type EmailSender interface {
	Send(to, subject, body string) error
}
