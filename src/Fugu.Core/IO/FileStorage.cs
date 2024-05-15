﻿namespace Fugu.IO;

public sealed class FileStorage : IBackingStorage, IDisposable
{
    private readonly string _path;
    private readonly object _sync = new();
    private readonly Dictionary<string, FileSlab> _slabs = new();

    public FileStorage(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        if (!Path.Exists(path))
        {
            throw new InvalidOperationException($"The provided path '{path}' does not exist.");
        }

        _path = path;
    }

    public ValueTask<IWritableSlab> CreateSlabAsync()
    {
        var fileName = Path.ChangeExtension(Path.GetRandomFileName(), "slab");
        var filePath = Path.Combine(_path, fileName);

        var slab = new FileSlab(filePath);

        _slabs.Add(filePath, slab);

        return ValueTask.FromResult<IWritableSlab>(slab);
    }

    public ValueTask<IReadOnlyCollection<ISlab>> GetAllSlabsAsync()
    {
        foreach (var filePath in Directory.EnumerateFiles(_path, "*.slab"))
        {
            if (!_slabs.ContainsKey(filePath))
            {
                // TODO: construct slab from filePath and add it to the dictionary.
            }
        }

        return ValueTask.FromResult<IReadOnlyCollection<ISlab>>([]);
    }

    public ValueTask RemoveSlabAsync(ISlab slab)
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
        foreach (var slab in _slabs.Values)
        {
            slab.Dispose();
        }

        _slabs.Clear();
    }
}
