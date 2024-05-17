# Outbox

## Gorm Example

Workaround for resolving `*sql.Tx` from Gorm `*gorm.DB`:

```go
err := gorm.DB.Transaction(func(tx *gorm.DB) error {
    pool := tx.Statement.ConnPool

    store.SaveTx(ctx, pool.(*sql.Tx), &outbox.Event{...})
})
```
