A vim mapping that helps when you are running the docker-compose rig from concourse/concourse:

```
nmap ,r :w<cr>:!go build && ./metricdrain --postgres-port 6543 --postgres-user dev --postgres-password dev --postgres-database concourse --emit-to-logs --log-level debug<cr>
```

It would be great to get this working with a prettier demo, so I copy-pasted a docker-compose config for running influxdb and grafana but I remember it being a bit tricky to salt the entire setup for that stack.
