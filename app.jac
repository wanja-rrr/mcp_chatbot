import std; # <--- Ensure this is correct and has a semicolon

walker init {
    x: int = 34 - 30;
    y: str = "Hello";
    z: float = 3.45;

    if (z == 3.0 or y == "bye") {
        x = x - 1;
        y: str = "world";
    }

    std.out(x);

    for i in std.range(3) {
        std.out(x - i, "-", y);
    }

    report([x, y + 's']);
}

with entry {
    std.out(spawn init);
}