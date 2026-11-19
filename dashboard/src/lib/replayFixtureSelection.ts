function distinctStrings(value: unknown): string[] {
  if (!Array.isArray(value) || !value.every((key: unknown) => typeof key === "string" && key.length > 0)) {
    throw new Error("Field keys must be nonempty strings.");
  }
  return [...new Set<string>(value)];
}

function usesJson(text: string): boolean {
  const first = text.trimStart()[0];
  return first === "{" || first === "[";
}

export function parseReplayFixtureSelection(input: string, output: string) {
  const inputKeys = usesJson(input)
    ? distinctStrings(JSON.parse(input))
    : distinctStrings(input.split(",").map((key) => key.trim()).filter(Boolean));
  const outputFields = new Map<string, string[]>();
  if (usesJson(output)) {
    const fields: unknown = JSON.parse(output);
    if (!fields || typeof fields !== "object" || Array.isArray(fields)) {
      throw new Error("Enter a JSON object mapping block IDs to field keys.");
    }
    for (const [block, keys] of Object.entries(fields)) {
      if (!block) throw new Error("Block IDs must be nonempty.");
      const selected = distinctStrings(keys);
      if (selected.length) outputFields.set(block, selected);
    }
  } else {
    for (const pair of output.split(",").map((key) => key.trim()).filter(Boolean)) {
      const separator = pair.indexOf(".");
      const block = pair.slice(0, separator).trim();
      const field = pair.slice(separator + 1).trim();
      if (separator < 1 || !block || !field) {
        throw new Error("Enter output fields as block.field, separated by commas, or use a JSON mapping.");
      }
      const keys = outputFields.get(block) ?? [];
      if (!keys.includes(field)) keys.push(field);
      outputFields.set(block, keys);
    }
  }
  return { input_keys: inputKeys, output_keys: Object.fromEntries(outputFields) };
}
