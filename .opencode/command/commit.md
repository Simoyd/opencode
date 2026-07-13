---
description: create a local git commit
model: opencode/kimi-k2.5
subtask: true
---

create a local commit and stop without pushing or performing remote integration

make sure it includes a prefix like
docs:
tui:
core:
ci:
ignore:
wip:

For anything in the packages/web use the docs: prefix.

prefer to explain WHY something was done from an end user perspective instead of
WHAT was done.

do not do generic messages like "improved agent experience" be very specific
about what user facing changes were made

if there are conflicts DO NOT FIX THEM. notify me and I will fix them

do not push, merge a remote branch, or otherwise publish the commit. report the
local commit when finished so the owning repository workflow can perform any
separately authorized publication step.

## GIT DIFF

!`git diff`

## GIT DIFF --cached

!`git diff --cached`

## GIT STATUS --short

!`git status --short`
