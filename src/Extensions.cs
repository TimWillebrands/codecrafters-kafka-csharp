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
}