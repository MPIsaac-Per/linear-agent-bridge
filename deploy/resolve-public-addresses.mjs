// Resolve a hostname through explicit public resolvers and, separately, through
// the system resolver, so the caller can tell the two answers apart.
//
// Split-horizon DNS is why this exists. On a host joined to the same overlay
// network as the service, the system resolver returns the node's private
// address and every probe travels a path the public internet cannot reach. The
// probe then succeeds and proves nothing.
//
// Reads VERIFY_HOST and VERIFY_RESOLVERS from the environment. Writes one
// record per line: "public <address>", "system <address>", or
// "resolver_failed <resolver> <error-code>". Exits 3 when no configured
// resolver could be reached, so the caller can say the public path was never
// tested rather than reporting success.
import { Resolver, promises as dnsPromises } from "node:dns";

const host = process.env.VERIFY_HOST;
const resolverList = (process.env.VERIFY_RESOLVERS ?? "")
  .split(",")
  .map((entry) => entry.trim())
  .filter(Boolean);

if (!host || resolverList.length === 0) {
  process.stderr.write("resolve-public-addresses: VERIFY_HOST and VERIFY_RESOLVERS are required\n");
  process.exit(2);
}

const QUERY_TIMEOUT_MS = 5_000;

function errorCode(error) {
  const code = error?.code ?? error?.errno ?? "UNKNOWN";
  // Bounded: resolver errors must not leak a message that echoes the query.
  return /^[A-Za-z0-9_]{1,32}$/.test(String(code)) ? String(code) : "UNKNOWN";
}

async function queryOne(server) {
  const resolver = new Resolver({ timeout: QUERY_TIMEOUT_MS, tries: 1 });
  resolver.setServers([server]);
  const answers = [];
  let answered = false;
  let lastError;
  for (const method of ["resolve4", "resolve6"]) {
    try {
      const records = await new Promise((resolve, reject) => {
        resolver[method](host, (error, result) => {
          if (error) reject(error);
          else resolve(result);
        });
      });
      answered = true;
      answers.push(...records);
    } catch (error) {
      // A host with only A records legitimately has no AAAA. That is an empty
      // answer from a reachable resolver, not an unreachable resolver.
      if (errorCode(error) === "ENODATA" || errorCode(error) === "ENOTFOUND") {
        answered = true;
      } else {
        lastError = error;
      }
    }
  }
  return { answered, answers, lastError };
}

const publicAddresses = new Set();
const lines = [];
let anyResolverAnswered = false;

for (const server of resolverList) {
  const { answered, answers, lastError } = await queryOne(server);
  if (answered) {
    anyResolverAnswered = true;
    for (const address of answers) {
      publicAddresses.add(address);
    }
  } else {
    lines.push(`resolver_failed ${server} ${errorCode(lastError)}`);
  }
}

for (const address of publicAddresses) {
  lines.push(`public ${address}`);
}

try {
  const systemAnswers = await dnsPromises.lookup(host, { all: true, verbatim: true });
  for (const entry of systemAnswers) {
    lines.push(`system ${entry.address}`);
  }
} catch (error) {
  lines.push(`system_failed ${errorCode(error)}`);
}

process.stdout.write(lines.join("\n") + (lines.length > 0 ? "\n" : ""));

if (!anyResolverAnswered) {
  process.exit(3);
}
