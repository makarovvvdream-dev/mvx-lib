# Unreleased

This version prepares the next `mvx-common` release.

## Added

* Added `PatternLogEventPolicy`, a ready-to-use logger event policy implementation based on ordered metadata pattern rules.
* Added `PatternLogEventPolicyAction`, `PatternLogEventPolicyRuleConfig`, and `PatternLogEventPolicyConfig`.
* Added mapping-based configuration constructors for pattern event policy rules and policy configuration.
* Added public re-exports for the pattern event policy API from `mvx.common.logger`.

## Documentation

* Added usage documentation for configuring logging width with `PatternLogEventPolicy`.
* Added architecture documentation for the pattern event policy decision model and runtime boundary.
* Added API documentation for the pattern event policy public objects.