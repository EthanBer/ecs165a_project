def foo(a: int, *arr: tuple[int, ...]) -> int:
    return 3

print(foo(1, 3, 4, 2))
