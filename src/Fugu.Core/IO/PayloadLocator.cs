namespace Fugu.Core.IO;

public readonly record struct PayloadLocator(
    long Start,
    int Size);
