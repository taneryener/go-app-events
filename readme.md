# Go App Events

This is a golang library aiming to be compatible with [decahedronio/laravel-app-events](https://github.com/decahedronio/laravel-app-events).

**Note**: This is a fork of the original [jobilla/go-app-events](https://github.com/jobilla/go-app-events) repository with additional improvements and updates.

The library allows you to send and listen to events coming from another service, with handlers
mapped to the various events.

A major difference between this library and the Laravel library is that protobuf messages
are **not** automatically decoded by the listener. Instead, the raw protobuf body (of type `[]byte`)
gets passed to the handler. The handler is then expected to run `proto.Unmarshal` into
the correct protobuf message type.

## Installation
```bash
go get github.com/taneryener/go-app-events
```

## Usage

This library relies on the [Go CDK](https://gocloud.dev) pubsub package. You should be
retrieving a pubsub topic using the CDK. The `topic` argument _does_ accept an interface,
so you _may_ provide an alternative topic that you create yourself or from another library.
For best compatibility, however, we recommend using the Go CDK.

### Dispatching an app event

```go
dispatcher := &Dispatcher{
    ctx: context.Background(),
    topic: pubsubTopic,
}

proto := &User{
    Name: "Rob Stark",
}

err := dispatcher.Dispatch("some.event", proto)
```
