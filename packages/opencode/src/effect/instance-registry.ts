const disposers = new Set<(directory: string) => Promise<void>>()

export function registerDisposer(disposer: (directory: string) => Promise<void>) {
  disposers.add(disposer)
  return () => {
    disposers.delete(disposer)
  }
}

export async function disposeInstance(directory: string) {
  let firstFailure: unknown
  for (const disposer of [...disposers].toReversed()) {
    try {
      await disposer(directory)
    } catch (error) {
      firstFailure ??= error
    }
  }
  if (firstFailure !== undefined) throw firstFailure
}
