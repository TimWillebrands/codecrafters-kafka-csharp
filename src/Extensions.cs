namespace CodecraftersKafka;

internal static class Extensions
{
    internal static ReadOnlySpan<byte> ToBytes(int value)
    {
        var intBytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(intBytes);
        return intBytes;   
    }

    internal static ReadOnlySpan<byte> ToBytes(short value)
    {
        var intBytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(intBytes);
        return intBytes;   
    }

    internal static MemoryStream Put(this MemoryStream ms, int value)
    {
        ms.Write(ToBytes(value));   
        return ms;
    }
    
    internal static MemoryStream Put(this MemoryStream ms, short value)
    {
        ms.Write(ToBytes(value));   
        return ms;
    }
    
    internal static MemoryStream Put(this MemoryStream ms, byte value)
    {
        ms.WriteByte(value);   
        return ms;
    }
    
    internal static MemoryStream PutArray(this MemoryStream ms, ReadOnlyMemory<byte> value)
    {
        ms.Put(value.Length);
        foreach (var b in value.Span)
        {
            ms.WriteByte(b);   
        }
        return ms;
    }
    
    internal static MemoryStream Put(this MemoryStream ms, ReadOnlyMemory<byte> value)
    {
        ms.Write(value.Span);   
        return ms;
    }
}