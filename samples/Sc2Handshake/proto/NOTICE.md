# Vendored: s2clientprotocol

`s2clientprotocol/*.proto` are vendored verbatim from **[Blizzard/s2client-proto](https://github.com/Blizzard/s2client-proto)**, licensed **MIT**. They are the StarCraft II AI protocol definitions and are FS-GG-owned by nobody — this is the "vendored external `.proto`" provenance from ADR-0052 §6.

They are committed (rather than fetched at build time) so the sample builds without network access, per the same convention FS.GG.Net's codegen guidance uses. Re-vendor from upstream when a new SC2 protocol version is targeted; the wire contract is Blizzard's, versioned independently of `FS.GG.Net`.
