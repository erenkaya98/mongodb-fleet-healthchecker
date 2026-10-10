# Bundled fonts

IBM Plex Sans and IBM Plex Mono, WOFF2, weights 400/500/600, `latin` and
`latin-ext` subsets. Taken from the [Fontsource](https://fontsource.org)
packages `@fontsource/ibm-plex-sans` and `@fontsource/ibm-plex-mono` (v5.3.0).

They are bundled rather than loaded from Google Fonts because this tool usually
runs inside a network with no outbound internet access, where a remote font
request never resolves.

Both subsets are needed for the Turkish UI: `latin` carries ı ç ö ü, while
ğ ş Ğ İ Ş are in `latin-ext`.

IBM Plex is © 2019 IBM Corp., licensed under the SIL Open Font License 1.1.
See `LICENSE-IBM-Plex-Sans.txt` and `LICENSE-IBM-Plex-Mono.txt`. The OFL is
independent of this project's MIT license, which covers the source code only.
