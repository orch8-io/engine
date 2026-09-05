export function validateSequenceJson(input: string): string
export function sequenceSchemaVersion(): number

/** Run an isolated dry-run workflow with a bounded scheduler tick budget. */
export function runSequenceJson(
  sequenceJson: string,
  inputJson?: string,
  maxTicks?: number,
): Promise<string>
