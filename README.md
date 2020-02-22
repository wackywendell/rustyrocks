If you see an error about "unknown type string", you may need:

```
SNPINC="/usr/local/Cellar/snappy/1.1.8/include/";CPLUS_INCLUDE_PATH="$SNPINC"
```

(The included snappy 1.1.4 attempts to use an internal `string`; 1.1.8 does not)
