# Verified sequence recipes

Use the matching CLI built from this checkout.

```sh
orch8 templates verify field-inspection
orch8 templates verify delivery-evidence
orch8 templates verify maintenance-approval

orch8 test run examples/verified-sequences/delivery-evidence.contracts.json \
  --report coverage --require-coverage
```

The recipes assert expected steps and terminal states under virtual time. The inspection case scripts a temporary transport failure. The delivery cases cover the normal and exception routes. The maintenance cases cover an approval, cancellation, and approval expiry.

The reports show the sequence version, required handlers, fixture results, and steps observed in passing cases. The fixtures use mocked handlers. Install and run the separate mobile starter to exercise the native runtime and a real connection.

To copy a recipe into an app project:

```sh
orch8 init my-workflow --template field-inspection
```

For a rollback example, run the saga compensation contract:

```sh
orch8 test run examples/verified-sequences/compensation.contracts.json \
  --report coverage --require-coverage
```
