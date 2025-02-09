using System.Buffers.Binary;
using System.Text;

namespace CodecraftersKafka;

internal static class Extensions
{
    internal static ReadOnlySpan<byte> ToBytes(long value)
    {
        var intBytes = BitConverter.GetBytes(value);
        if (BitConverter.IsLittleEndian)
            Array.Reverse(intBytes);
        return intBytes;   
    }

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

    internal static MemoryStream Put(this MemoryStream ms, long value)
    {
        ms.Write(ToBytes(value));   
        return ms;
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
    
    internal static MemoryStream PutCompactArray(this MemoryStream ms, ReadOnlyMemory<byte> value)
    {
        ms
            .Put(VarintDecoder.EncodeUnsignedVarint(value.Length)) // Length of compact arr is an unsigned-varint
            .Put(value);
        return ms;
    }
    
    internal static MemoryStream Put(this MemoryStream ms, ReadOnlyMemory<byte> value)
    {
        ms.Write(value.Span);   
        return ms;
    }

    internal static byte ReadByte(this ReadOnlyMemory<byte> buffer, ref int offset, string? label = null)
    {
        var value = buffer.Span[offset++];

        if (label is not null)
        {
            Console.WriteLine($"[ReadByte]\t{label}: ({value}|{value:X2})");
        }

        return value;
    }

    internal static short ReadShort(this ReadOnlyMemory<byte> buffer, ref int offset, string? label = null)
   {
        var value = BinaryPrimitives.ReadInt16BigEndian(buffer.Span[offset..(offset+=2)]);
        
        if (label is not null)
        {
            Console.WriteLine($"[ReadInt]\t{label}: ({value}|{value:X2})");
        }

        return value;
    }

    internal static int ReadInt(this ReadOnlyMemory<byte> buffer, ref int offset, string? label = null)
    {
        var value = BinaryPrimitives.ReadInt32BigEndian(buffer.Span[offset..(offset+=4)]);
        
        if (label is not null)
        {
            Console.WriteLine($"[ReadInt]\t{label}: ({value}|{value:X2})");
        }

        return value;
    }

    internal static long ReadLong(this ReadOnlyMemory<byte> buffer, ref int offset, string? label = null)
    {
        var value = BinaryPrimitives.ReadInt32BigEndian(buffer.Span[offset..(offset+=8)]);

        if (label is not null)
        {
            Console.WriteLine($"[ReadLong]\t{label}: ({value}|{value:X2})");
        }

        return value;
    }

    internal static int ReadVarUInt(this ReadOnlyMemory<byte> buffer, ref int offset, string? label = null)
    {
        var value = VarintDecoder.ReadUnsignedVarint(buffer.Span, ref offset);
        
        if (label is not null)
        {
            Console.WriteLine($"[ReadVarUInt]\t{label}: ({value}|{value:X2})");
        }

        return value;
    }

    internal static ReadOnlyMemory<byte> ReadNullableString(
        this ReadOnlyMemory<byte> buffer, 
        ref int offset, 
        string? label = null)
    {
        var len = BinaryPrimitives.ReadInt16BigEndian(buffer.Span[offset..(offset+=2)]);
        var str = buffer[offset..(offset += len)];
        
        if (label is not null)
        {
            var bytes = string.Join(" ", str.Span.ToArray().Select(b => b.ToString("X2")));
            Console.WriteLine($"[ReadNullableString]\t{label}: ({Encoding.UTF8.GetString(str.Span)}|{bytes})");
        }

        return str;
    }
}