# Outbox

## Gorm Example

Workaround for resolving `*sql.Tx` from Gorm `*gorm.DB`:

```go
err := gorm.DB.Transaction(func(tx *gorm.DB) error {
    pool := tx.Statement.ConnPool

    store.SaveTx(ctx, pool.(*sql.Tx), &outbox.Event{...})
})
```

## Testing

The package includes a test suite that you can run with the `go test` command:

```bash
go test ./...
```

## Contributing

Contributions to `go-outbox` are welcome. Please submit a pull request or create an issue to discuss the changes you want to make.

## License

`go-outbox` is licensed under the MIT License. See the `LICENSE` file for more information.
