# Context naming rules

A context name consists of one or more segments separated by dots.

Each segment must start with a letter. After the first letter, letters, digits, `_`, and `-` may be used.

Valid names:

```text
my-app
my_app.worker
my_app.worker_tasks
my-app.worker-1
```

Invalid names:

```text
1_app
my app
my_app.
.my_app
my_app..worker
```

An empty name is not used through the public API for ordinary contexts. It is reserved inside the package for the root context.
