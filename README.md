# dsh-subagent-concurrency-limit

Limit how many DeepSeek Harness subagent runs and local child-agent turns run at
the same time in one process. The root Agent is not counted.

## Why it is needed

In [DeepSeek Harness][dsh-repository] `dsh-v0.1.1-rc.2` (commit
[`b150a551b8d465e31e418e1b2eaf5e79bbb7d28e`][dsh-commit]), the workflow
engine's [`maxConcurrentAgents` and `maxTotalAgents`][dsh-workflow] apply to
`agent()` calls inside each workflow run. The standard
[`subagent` tool][dsh-subagent] has no process-wide limit for root-level direct
runs outside a workflow. [`maxParallelToolCalls`][dsh-tool-calls] limits the
dispatch pool for parallel-safe tool calls in one Agent step, not the number of
background or continuable runs.

This plugin fills that gap by limiting concurrent root delegation admissions and
local descendant Agent turns in one process.

## Scope

The plugin limits the delegation tools listed in `subagentToolNames` and local
Agent turns in the same process where `delegationDepthOf(agent) > 0`. They share
one process-local pool. The default limit is `2`.

- A root Agent's delegation call waits in FIFO order before creating a child.
- Add nonstandard delegation tool names to `subagentToolNames`.
- Nested children that call the same delegation tool also consume the pool. If a
  permit is available, they take it immediately; when the pool is full, they
  fail without waiting so a child cannot block the ancestor that is waiting for
  it.
- Workflow `agent()` calls and resumed local child turns use the same pool when
  their Agent turn starts.
- A permit is released when the child turn becomes idle, the run ends, or the
  Agent is disposed.
- For remote-process providers such as Codex, Claude Code, and ACP, only the
  configured root delegation admission and the observable `subagent/start` and
  `subagent/end` boundaries are visible. Nested execution and restarts inside
  the remote process cannot be counted.
- `maxParallelToolCalls` is independent because it limits tool calls, not live
  runs.

## Install

These examples use the `web` profile. Replace `web` with your profile name in
each command and path.

Install the plugin:

```sh
dsh plugin --profile web add \
  github:blauerberg/dsh-subagent-concurrency-limit#01182275695067510a187cb05b46df49e5fe812c
```

Git installs fetch the source and build `lib/` with the package's `prepare`
script. With pnpm 10 or later, the first command may stop because that build is
not allowed. Add the revision-specific key to the profile's workspace file
(keep any existing `allowBuilds` entries):

```yaml
# $DSH_HOME/profiles/web/pnpm-workspace.yaml
allowBuilds:
  'dsh-subagent-concurrency-limit@https://codeload.github.com/blauerberg/dsh-subagent-concurrency-limit/tar.gz/01182275695067510a187cb05b46df49e5fe812c': true
```

Save the file, then rerun the install command above. The commit hash used in
both places identifies `v0.1.0`. Installing another revision requires its
commit hash in both places.

Add the plugin configuration to
`$DSH_HOME/profiles/web/cordis.patch.yml`:

```yaml
- id: subagent-concurrency-limit
  name: dsh-subagent-concurrency-limit
  config:
    maxConcurrentSubagents: 2
    subagentToolNames:
      - subagent
```

`maxConcurrentSubagents` must be a positive safe integer.
`subagentToolNames` must contain at least one nonempty delegation tool name.
The bundle patch applies the defaults above; edit the same keys to override
them.

Start the profile:

```sh
dsh --profile web
```

## Development

```sh
pnpm install
pnpm run check
```

## License

The project is licensed under [MIT](LICENSE).

[dsh-repository]: https://github.com/deepseek-ai/deepseek-harness
[dsh-commit]: https://github.com/deepseek-ai/deepseek-harness/tree/b150a551b8d465e31e418e1b2eaf5e79bbb7d28e
[dsh-workflow]: https://github.com/deepseek-ai/deepseek-harness/blob/b150a551b8d465e31e418e1b2eaf5e79bbb7d28e/packages/workflow/workflow-worker-thread/src/index.ts
[dsh-subagent]: https://github.com/deepseek-ai/deepseek-harness/blob/b150a551b8d465e31e418e1b2eaf5e79bbb7d28e/packages/subagent/tool-subagent/src/index.ts
[dsh-tool-calls]: https://github.com/deepseek-ai/deepseek-harness/blob/b150a551b8d465e31e418e1b2eaf5e79bbb7d28e/packages/core/agent-loop/src/tool-calls.ts
