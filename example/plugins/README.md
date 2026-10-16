# Plugin registration check

The checker registers a named native workflow from a plugin inherited from the
client and a dynamic native workflow from an explicit worker plugin. It verifies
both results against a local development server, then registers through replay
plugins and replays both histories. It runs with workflow cache sizes 1000 and 0.

From the SDK root:

```sh
PLUGIN_DEV_SERVER=1 tgo test ./example/plugins/check -run TestLivePluginRegistrations -count=1
```
