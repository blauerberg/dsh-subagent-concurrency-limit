# dsh-subagent-concurrency-limit

Limit how many DeepSeek Harness subagent runs and local child-agent turns run at
the same time in one process. The root Agent is not counted.

## Scope

Configured delegation tools and local Agent turns where
`delegationDepthOf(agent) > 0` share one process-local pool.

- Root delegation calls wait in FIFO order before creating a child.
- Nested delegation takes a free permit or fails immediately. Waiting could
  deadlock an ancestor that is collecting the child's result.
- Local child turns reuse their delegation reservation. Workflow children and
  resumed turns without a published run enter the same FIFO queue.
- A published local child without a reservation takes a free permit or fails
  immediately, because an ancestor may already be waiting for it.
- Permits are released on idle, run completion, Agent disposal, or a tool
  failure before child publication. Both thrown errors and error results count.
- Background jobs retain their reservation until the child is published. When
  the optional Harness `jobs` service is available, its registration and
  settlement events release reservations for jobs that end before publication.
- Remote providers are counted from configured delegation admission through
  the observable `subagent/end` event. Execution inside a remote process is
  outside this pool's scope.

Custom delegation tools must publish `subagent/start` in the delegating call's
async context and pair it with `subagent/end`. A deferred tool that succeeds
without publishing a child or registering a Harness subagent job keeps its
reservation until the delegating Agent is disposed.

Pool waits time out after `queueTimeoutMs`. Harness `maxParallelToolCalls`
controls tool dispatch separately from this pool.

## Install

These examples use the `web` profile. Replace `web` with your profile name in
each command and path.

Install the plugin:

```sh
dsh plugin --profile web add dsh-subagent-concurrency-limit
```

Add the plugin configuration to
`$DSH_HOME/profiles/web/cordis.patch.yml`:

```yaml
- id: subagent-concurrency-limit
  name: dsh-subagent-concurrency-limit
  config:
    maxConcurrentSubagents: 2
    queueTimeoutMs: 300000
    subagentToolNames:
      - subagent
```

| Setting | Default | Accepted values |
| --- | --- | --- |
| `maxConcurrentSubagents` | `2` | Positive safe integer |
| `queueTimeoutMs` | `300000` | Integer from `1` to `2147483647` milliseconds |
| `subagentToolNames` | `['subagent']` | Nonempty array of nonempty tool names |

The bundle patch applies these defaults. Edit the same keys to override them.
The timeout maximum is the Node.js timer limit.

Start the profile:

```sh
dsh --profile web
```

## Update

To update the installed plugin to the latest npm release, run:

```sh
dsh plugin --profile web add dsh-subagent-concurrency-limit@latest
```

Restart the profile after updating.

## Development

```sh
pnpm install
pnpm run check
```

`check` runs lint, typechecks both source and tests, runs the tests, and builds
`lib/`. CI checks the locked dependencies and the latest Harness release in
separate jobs.

To inspect the package before publishing:

```sh
npm pack --dry-run
```

`prepare` builds `lib/` when packing or publishing. `npm publish` runs the
checks through `prepublishOnly` before publishing to npm.

## License

The project is licensed under [MIT](LICENSE).

## Harness compatibility

The declared compatibility range is DeepSeek Harness `>=0.2.0-rc.2` with Cordis
`^4.0.4`. The `@deepseek-ai/dsh-jobs` peer is optional; local turns and direct
runs also work without a jobs service.
