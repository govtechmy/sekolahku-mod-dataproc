### Tinybird

Prerequisites: Tinybird Forward (not Tinybird Classic)

### Change directory:
```
cd tinybird/
```

### Login
Cli login to authenticate:
```
tb login --host https://api.us-east.tinybird.co 
```

### To create new datasource

```
tb datasource create
```

### To push changes from local to Tinybird
Deploy datasources and pipes
```
tb --cloud deploy 
```

If theres any datasources/pipes deletion (use with caution):
```
tb --cloud deploy --allow-destructive-operations
```
