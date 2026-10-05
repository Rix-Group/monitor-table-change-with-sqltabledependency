// PreToolUse hook: blocks any agent attempt to push commits, on any branch, in any repo.
let input = '';
process.stdin.on('data', c => (input += c));
process.stdin.on('end', () => {
  let event = {};
  try { event = JSON.parse(input); } catch { process.exit(0); }

  const tool = event.tool_name ?? '';
  const command = String(event.tool_input?.command ?? '');

  // Any MCP tool whose name is a push (GitKraken git_push and the like).
  const mcpPush = /^mcp__.*push/i.test(tool);

  // `git push`, `git -C dir push`, `git -c k=v push`, `git.exe push`, inside any chain, subshell or script string.
  const shellPush = /(Bash|PowerShell)/.test(tool) && /\bgit(\.exe)?\b(\s+(-[Cc]\s+\S+|--?[\w-]+(=\S+)?))*\s+push\b/i.test(command);

  // Pushing through aliases or other front ends.
  const otherPush = /(Bash|PowerShell)/.test(tool) && /\b(gh\s+repo\s+sync|git\s+subtree\s+push|hub\s+push)\b/i.test(command);

  if (mcpPush || shellPush || otherPush) {
    process.stderr.write('Blocked: agents must never push commits, on any branch. Commit locally and tell the user; the user pushes.\n');
    process.exit(2);
  }
  process.exit(0);
});