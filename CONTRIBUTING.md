# Contributing to Linear Agent Bridge

Thank you for your interest in improving Linear Agent Bridge! We welcome bug reports, documentation enhancements, and feature contributions.

## Prerequisites & Development Setup

- **Node.js**: `>= 22.0.0`
- **Package Manager**: `npm`

```bash
git clone https://github.com/MPIsaac-Per/linear-agent-bridge.git
cd linear-agent-bridge
npm install
```

## Architecture & Hard Constraints

Linear Agent Bridge connects Linear webhook events and agent session tokens to a local agent runtime.

### Serial Execution Constraint
To avoid race conditions and maintain state determinism across agent sessions, **agent execution jobs are strictly processed serially**. Concurrent dispatch queues must not bypass the sequential execution lock.

## Development Workflow & Verification

Before submitting a Pull Request, ensure that all types and tests pass cleanly:

```bash
# Type check
npm run typecheck

# Run test suite
npm test

# Run tests in watch mode during development
npm run test:watch
```

## Pull Request Guidelines

1. Create a focused branch: `git checkout -b feature/your-feature-name` or `fix/your-fix-name`.
2. Follow Test-Driven Development (TDD): Add tests covering any bug fix or new behavior.
3. Ensure `npm run typecheck` and `npm test` pass with 0 errors.
4. Provide a clear PR description detailing motivation, scope, and verification steps.
5. All commits must be DCO signed (`git commit -s`).
