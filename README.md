# Flux Hierarchy

> Create trees of Flux instances

🚧 **under development and experimental** 🚧

[![PyPI version](https://img.shields.io/pypi/v/flux-hierarchy)](https://img.shields.io/pypi/v/flux-hierarchy)

![https://github.com/converged-computing/flux-hierarchy/blob/main/img/flux-hierarchy-small.png?raw=true](https://github.com/converged-computing/flux-hierarchy/blob/main/img/flux-hierarchy-small.png?raw=true)

This tool enables generation and orchestration of Flux hierarchies, or trees of instances.
Such a setup can enable programmatic organization and submission of commands, or high
throughput. Use cases we want to address:

- Creation (and organization) of a Flux Hierarchy
- Discovery of an existing Flux Hierarchy (e.g, for MCP)

## Usage

Let's first create a hierarchy. This will be a Flux job. You'll need to be in a Flux instance where a handle is discoverable. E.g., in the DevContainer:

```bash
flux start
```

Then create a simple, flat hierarchy with all the resources allocated to one broker.

```bash
flux-hierarchy start ./examples/hierarchy-one.yaml
```

You can test throughput (this also starts the hierarchy):

```bash
flux-hierarchy throughput ./examples/hierarchy-one.yaml
```

For either of the above, the hierarchy will continue running (and you need to cancel the job).

```bash
flux cancel $(flux job last)
```

You can also view the shape of the hierarchy without running anything:

```bash
flux-hierarchy view ./examples/hierarchy-one.yaml
```
```console
$ flux-hierarchy view ./examples/corona/hierarchy-2.yaml
=>
🌿 Leaf Broker Workers...{}
level1 [Nodes: 2]
    ├── level2 [Nodes: 1, Cores: 48]
    └── level2 [Nodes: 1, Cores: 48]
```

## Development

To build and release:

```bash
python3 -m build
# or
python3 setup.py sdist bdist_wheel

twine upload dist/flux-hierarchy-<version>*
```

## WIP / TODO / Would be nice

- I can't remember command to get `<host>:<rank>` mapping (I came up with something)
- Use kvs for uris, saving results, etc. instead of the local dir.
- Have local throughput wait for results not rely on filesystem results (use job wait)
- Some means to deploy submit to node as a service on the node (that knows about URIs)
- Save result to kvs or similar (not filesystem)
- Should be able to read in directory of active sockets to generate tree
- Allow different job shapes / specs.
- Expose simulation duration time
- Expose other resource params
- A check for non-local mode that we aren't going to ddos ssh.

## License

HPCIC DevTools is distributed under the terms of the MIT license.
All new contributions must be made under this license.

See [LICENSE](https://github.com/converged-computing/cloud-select/blob/main/LICENSE),
[COPYRIGHT](https://github.com/converged-computing/cloud-select/blob/main/COPYRIGHT), and
[NOTICE](https://github.com/converged-computing/cloud-select/blob/main/NOTICE) for details.

SPDX-License-Identifier: (MIT)

LLNL-CODE- 842614
