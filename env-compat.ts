/** Parley 1.1 renamed the `PI_INTERCOM_*` environment surface to `PI_PARLEY_*`.
 * Launchers and shells across the fleet still export the old names for now, so
 * every read goes through both: the new name wins, the legacy name is honored
 * through the transition. */
export function parleyEnv(
  name: string,
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  return env[name] ?? env[name.replace(/^PI_PARLEY_/, "PI_INTERCOM_")];
}
