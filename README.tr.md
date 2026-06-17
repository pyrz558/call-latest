# call-latest

Only the latest async call should win.

## Links

- [npm](https://www.npmjs.com/package/call-latest)
- [Documentation](./README.md)

## Install

```bash
npm install call-latest
```

## Quick example

```ts
import { latest, isStale } from "call-latest";

const search = latest(async (query: string) => {
  const res = await fetch(`/api/search?q=${query}`);
  return res.json();
});

try {
  await search("react");
} catch (err) {
  if (!isStale(err)) throw err;
}
```

See [README.md](./README.md) for full documentation.
