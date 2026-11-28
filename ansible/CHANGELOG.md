# Changelog

## 0.1.0

- Roles `operator`, `host`, `nomad`, `nomad_receiver`, `kubernetes`, `kubernetes_receiver`, `systemd` and `systemd_receiver`, each with `wherry_<role>_state: present` or `absent`; `absent` needs the role's own `wherry_<role>_confirm_absent: true`.
- Role arguments are `wherry_<role>_*`; shared values are collection variables (`wherry_version`, `wherry_tenant`, `wherry_<plugin>_plugin_version` and others) that the role defaults read.
