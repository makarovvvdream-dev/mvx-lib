# From the author

```{image} ../../_static/vladimir_makarov.jpeg
:alt: Vladimir Makarov
:width: 180px
:align: right
```

This logger appeared because I needed it for a real project.

I looked for an existing solution, but did not find one that matched the shape I wanted: structured events, explicit payload control, context-based configuration, predictable sink boundaries, and operation-level logging through `log_invocation`, while still remaining lightweight, flexible, and free from heavy infrastructure or administration requirements.

So I built the component I needed.

The current functionality reflects the needs of the project at this stage. It is not a claim that the logger already covers every possible logging scenario. It is a practical implementation with clear boundaries and enough room to grow.

The design is intentionally modular. Some users may only need the ready-to-use sinks and package-level configuration. Others may want to use `LogContext` directly, write their own sinks, customize payload handling, or use `log_invocation` at public API boundaries.

I believe this gives the project good potential for further development, as long as new features grow from real use cases rather than from feature collecting.

This is my first public project. I tried to make the documentation useful and to cover the code with meaningful tests.

Still, mistakes are possible. If you find an error in the code or in the documentation, please do not hesitate to contact me immediately.

I am open to collaboration, feedback, and practical feature requests. If you are using this logger, evaluating it, or missing something important for your project, I am happy to discuss it.

Source code is available on [GitHub](https://github.com/makarovvvdream-dev/mvx-lib).
For bugs and feature requests, please use GitHub Issues.
For collaboration or direct discussion, contact me at makarovvv.dream@gmail.com.

All the best,

Vladimir Makarov
