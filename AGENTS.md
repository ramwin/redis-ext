# AI Agent Development Guide for redis-ext

This file contains essential information for AI coding agents working with the redis-ext project. It provides context about the architecture, development workflow, and conventions specific to this codebase.

## Project Overview

**redis-ext-ramwin** is a Python library that provides Redis-based extensions for distributed systems. It includes:

- **RedisWork**: Thread-pool based Redis list consumer for processing tasks concurrently
- **DebounceInfoTask**: Distributed task scheduler with deduplication using Redis Sorted Sets
- **FastSet**: Redis-backed distributed set implementation with local caching
- **UniqueId**: Distributed unique ID generator using Redis atomic operations

### Key Information
- **Version**: 0.6.0
- **Author**: Xiang Wang (ramwin@qq.com)
- **License**: MIT
- **Minimum Python**: 3.8
- **Primary Dependency**: redis >= 5.0

## Technology Stack

### Core Dependencies
- **Redis Client**: `redis-py` (>=5.0) for Redis interactions
- **Python Type Hints**: Heavy use of generics and type annotations throughout
- **Threading**: Built-in threading support for concurrent task processing

### Development Tools
- **Build System**: Hatchling (modern Python build backend)
- **Test Framework**: pytest with fakeredis for Redis mocking
- **Linting**: Ruff with custom rule set (E, F, I, N, UP, B, C90)
- **Code Coverage**: pytest-cov integration

## Project Structure

```
src/redis_ext/
├── __about__.py      # Version information (0.6.0)
├── __init__.py       # Public API exports (RedisWork)
├── tasks.py          # Task-related exports (DebounceInfoTask)
├── types.py          # Type definitions (RedisClient union type)
├── worker.py         # RedisWork - 120 lines
├── debounce_task.py  # DebounceInfoTask - 518 lines
├── fastset.py        # FastSet - 147 lines
├── unique_id.py      # UniqueId - 30 lines
└── py.typed          # Type marker file (PEP 561)

tests/
├── test_worker.py      # RedisWork tests - 104 lines
├── test_fastset.py     # FastSet tests - 55 lines
└── test_unique_id.py   # UniqueId tests - 33 lines
```

## Architecture Details

### RedisWork (Generic Task Processor)
- **Purpose**: Listen to Redis lists and process tasks using thread pools
- **Key Features**:
  - Generic type support for task data
  - Configurable worker pool size
  - Graceful shutdown handling
  - Error propagation stops processing on first exception
  - Concurrency limiting with condition variables
  - JSON serialization/deserialization for tasks
- **Usage Pattern**: Subclass `RedisWork[T]` and implement `handle(self, task: T) -> Any`
- **Important**: Tasks must be JSON-serializable dictionaries

### DebounceInfoTask (Distributed Task Scheduler)
- **Purpose**: Schedule tasks with deduplication using Redis Sorted Sets
- **Key Features**:
  - Content-based deduplication (serialized JSON comparison)
  - Configurable delay/schedule time per task
  - Batch task retrieval
  - Task statistics and monitoring
  - Support for complex nested data structures
- **Design**: Uses Redis ZSET with timestamps as scores
- **Deduplication**: Based on JSON serialization with sorted keys

### FastSet (Distributed Set with Caching)
- **Purpose**: Redis-backed set implementation with local caching
- **Key Features**:
  - Generic type support
  - Configurable cache timeout
  - Optimistic locking with version checking
  - Full set operation support (union, intersection, difference, etc.)
  - Atomic operations using Redis pipelines
- **Caching Strategy**: Lazy refresh based on timeout and version mismatch
- **Consistency**: Uses version keys to detect remote changes

### UniqueId (Distributed ID Generator)
- **Purpose**: Generate unique IDs using Redis atomic operations
- **Key Features**:
  - Class-based ID spaces (isolated by key prefix)
  - Configurable expiration for ID mappings
  - Thread-safe using Redis SET with NX flag
  - Auto-incrementing counter per class
- **Use Case**: Create unique identifiers for entities in distributed systems

## Development Commands

### Testing
```bash
# Run tests with quiet output
hatch run test
# or
pytest -q

# Run tests with coverage report
hatch run cov
# or
pytest --cov=redis_ext --cov-report=term-missing

# Run specific test file
pytest tests/test_worker.py -v

# Run tests with fakeredis (no Redis server needed)
# All tests use fakeredis fixtures - no external dependencies
```

### Linting and Code Quality
```bash
# Run ruff linter
ruff check src/ tests/

# Auto-fix linting issues
ruff check --fix src/ tests/

# Check line length (max: 88 characters)
ruff check --select=E501 src/ tests/
```

### Building
```bash
# Build package (uses hatchling)
hatch build

# Install in development mode
pip install -e .

# Install with test dependencies
pip install -e ".[test]"
```

## Code Style Guidelines

### Type Annotations
- **Heavy use of generics**: All major classes use Generic[T] for type safety
- **RedisClient type**: Use `RedisClient` union type from `types.py` for Redis clients
- **TypeVar declarations**: Follow the pattern `T = TypeVar('T')` at module level

### Naming Conventions
- **Classes**: PascalCase (e.g., `RedisWork`, `DebounceInfoTask`)
- **Methods**: snake_case (e.g., `handle`, `add_task`, `get_or_create`)
- **Private methods**: Leading underscore (e.g., `_safe_handle`, `_fetch`)
- **Redis keys**: Use colon separator pattern (e.g., `f"{key}:value"`, `f"{klass}:id:{key}"`)
- **Generic types**: Single capital letters (T, TaskT)

### Code Organization
- **Docstrings**: Multi-line docstrings for classes, single-line for simple methods
- **Comments**: Chinese comments in worker.py (understand project context), English elsewhere
- **Imports**: Group standard library, third-party, and local imports
- **Line length**: Maximum 88 characters (Black/ruff default)

### Error Handling
- **RedisWork**: Logs errors, stops on first exception, stores exception in `_first_exc`
- **FastSet**: Retries on version conflicts, refreshes cache on errors
- **DebounceInfoTask**: JSON serialization errors handled gracefully
- **UniqueId**: Uses atomic operations with retry loops

## Testing Strategy

### Test Structure
- **Fixture Pattern**: All tests use `fake_redis` fixture with `fakeredis.FakeRedis`
- **Test Isolation**: Each test gets fresh Redis state via fixtures
- **Async Testing**: Tests use threading for concurrent execution testing
- **Mock Redis**: No external Redis server required for testing

### Test Coverage
- **worker.py**: Tests basic flow, error handling, max worker limits, concurrent execution
- **debounce_task.py**: Comprehensive tests including deduplication, delays, batch operations, statistics
- **fastset.py**: Tests CRUD operations, set operations, version management, caching
- **unique_id.py**: Tests ID generation, expiration, class isolation

### Writing New Tests
1. Always use the `fake_redis` fixture
2. Test both success and error paths
3. Include timeout-based tests for time-sensitive features
4. Test concurrent behavior where applicable
5. Clean up test data after tests

## Security Considerations

### Redis Security
- No authentication configured (assumes trusted environment or external auth)
- Timeout-based expiration used for temporary data
- Uses Redis pipelines for atomic operations

### Input Validation
- **JSON Deserialization**: Wrapped in try/except in worker.py
- **Task Data**: Assumed to be JSON-serializable, validated at runtime
- **Redis Keys**: User-provided keys are used directly (injection risk if untrusted)

### Concurrency Safety
- Thread-safe operations using locks and condition variables
- Redis atomic operations for critical sections
- Version-based optimistic locking in FastSet

## Common Development Tasks

### Adding a New Redis-based Component
1. Create new file in `src/redis_ext/`
2. Add type definitions to `types.py` if needed
3. Export from `__init__.py` or create dedicated exports file
4. Add comprehensive tests using fakeredis fixtures
5. Update AGENTS.md with component details

### Modifying Existing Components
1. Maintain backward compatibility for public APIs
2. Update tests for new behavior
3. Follow existing patterns for error handling and logging
4. Consider thread-safety implications
5. Update version in `__about__.py` for significant changes

### Debugging Redis Operations
1. Enable Redis logging: `import logging; logging.basicConfig(level=logging.DEBUG)`
2. Use `fakeredis` for reproducible test cases
3. Check Redis pipelines execution order
4. Monitor version increments for cache consistency

## Performance Considerations

### RedisWork
- `max_workers` limits concurrent processing (default: 4)
- `timeout` controls blocking time on Redis BLPOP (default: 5s)
- Condition variables prevent busy-waiting when thread pool is full

### DebounceInfoTask
- `pop_tasks(count)` controls batch size (default: 10)
- JSON serialization overhead for deduplication
- Use appropriate delay_seconds for your use case

### FastSet
- `timeout` parameter controls cache refresh frequency
- Version checks add Redis round-trips on conflicts
- Bulk operations (update, clear) more efficient than individual adds

### UniqueId
- Redis INCR operation is atomic but network-bound
- Use appropriate timeout for ID mappings based on use case
- Consider ID space isolation using different `klass` values
