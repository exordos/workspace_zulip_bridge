# Production bridge release workflow

The `Exordos element` workflow retains its existing default behavior for pull
requests, pushes, tags, and ordinary manual runs: it builds one element and
publishes it with the configured repository credentials. Operators use the
manual `production_release` profile only when preparing the immutable Provider
bridge version required by the Workspace PostgreSQL-canonical cutover.

The profile requires two repository secrets:

- `PUSH_CFG`: the base64-encoded private Exordos repository push config;
- `WORKSPACE_BRIDGE_RELEASE_EVIDENCE_DIR`: an owner-controlled path on durable,
  access-controlled storage attached to the self-hosted runner.

Neither secret is accepted as a workflow input. The evidence directory, push
configuration, rendered internal resource identifiers, and private push output
are never printed or uploaded as GitHub artifacts.

The production profile:

1. binds the release to the checked-out source commit and tree;
2. creates an empty local build commit and a unique, time-ordered `rc` version
   tag without changing the source tree;
3. builds into a new run-specific directory without a force overwrite;
4. verifies the rendered element name and version, parses the inventory, checks
   every compressed image with `zstd -t`, and creates a SHA-256 inventory of the
   complete build tree;
5. atomically finalizes an owner-only evidence directory in the `prepared`
   state before repository publication starts;
6. pushes without `--force`, keeping all command output in the private evidence
   directory;
7. changes the evidence state to `published` or `publication_failed`, adds the
   private push log to the checksum inventory, and fails the job on any
   collision or publication error;
8. reads the terminal state and checksum inventory back from the finalized
   archive, rechecks the exact version and source bindings, and exposes the safe
   version output only after that readback passes.

The evidence bundle includes the original source commit and tree, the empty
release commit, exact version, pinned Exordos CLI version and download digest,
rendered manifest, inventory, compression result, artifact SHA-256 inventory,
run identity, timestamps, publication state, private push log, and an
`evidence.sha256` file covering the bundle. All directories are mode `0700` and
all files are mode `0600`.

Only the exact release version is exposed as a safe workflow output. Before a
cutover, an operator must read back the private evidence, verify
`evidence.sha256`, refresh the configured Exordos repository, and prove that
the exact `workspace_zulip_bridge` version is available. A successful workflow
run by itself is not deployment authorization.
