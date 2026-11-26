# Control plane

The managed control plane is a separate, closed product operated by Diviga. It
is not required to run Actae.

The community build in this repository runs standalone: no account, no
enrollment, no license check. The only reason to interact with a control plane
is to accept the managed offering.

The client code still lives in `src/control_plane/`; removing it from the
community build is tracked in [control-plane-split.md](control-plane-split.md).
