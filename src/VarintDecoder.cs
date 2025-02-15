using System.Runtime.InteropServices;

namespace CodecraftersKafka;

public static class VarintDecoder
{
    public static int ReadUnsignedVarint(ReadOnlySpan<sbyte> buffer, ref int offset)
    {
        var tmp = buffer[offset++];
        if (tmp >= 0)
        {
            return tmp;
        }
        
        var result = tmp & 127;
        tmp = buffer[offset++];
        if (tmp >= 0)
        {
            result |= tmp << 7;
        }
        else
        {
            result |= (tmp & 127) << 7;
            tmp = buffer[offset++];
            if (tmp >= 0)
            {
                result |= tmp << 14;
            }
            else
            {
                result |= (tmp & 127) << 14;
                tmp = buffer[offset++];
                if (tmp >= 0)
                {
                    result |= tmp << 21;
                }
                else
                {
                    result |= (tmp & 127) << 21;
                    tmp = buffer[offset++];
                    result |= tmp << 28;
                    if (tmp < 0)
                    {
                        throw CreateIllegalVarintException(result);
                    }
                }
            }
        }
        return result;
    }
    
    public static int ReadSignedVarint(ReadOnlySpan<sbyte> buffer, ref int offset) {
        var value = ReadUnsignedVarint(buffer, ref offset);
        return (value >>> 1) ^ -(value & 1);
    }
    
    public static (int Value, int Length) ReadUnsignedVarint(ReadOnlySpan<sbyte> buffer)
    {
        var offset = 0;
        return (ReadUnsignedVarint(buffer, ref offset), offset);
    }

    public static (int Value, int Length) ReadUnsignedVarint(ReadOnlySpan<byte> buffer)
        => ReadUnsignedVarint(MemoryMarshal.Cast<byte, sbyte>(buffer));
    
    public static (int Value, int Length) ReadSignedVarint(ReadOnlySpan<sbyte> buffer)
    {
        var offset = 0;
        return (ReadSignedVarint(buffer, ref offset), offset);
    }
    
    public static int ReadUnsignedVarint(ReadOnlySpan<byte> buffer, ref int offset) 
        => ReadUnsignedVarint(MemoryMarshal.Cast<byte, sbyte>(buffer), ref offset);
    
    private static InvalidOperationException CreateIllegalVarintException(int result)
    {
        return new InvalidOperationException($"Illegal varint value: {result}");
    }
    
    public static byte[] EncodeSignedVarint(int value)
    {
        var ziggityZaggity = (uint)((value << 1) ^ (value >> 31));
        return EncodeUnsignedVarint(ziggityZaggity);
    }
    
    public static byte[] EncodeUnsignedVarint(uint value)
    {
        var bytes = new List<byte>();
        while (value >= 128)
        {
            bytes.Add((byte)((value & 0x7F) | 0x80)); // Take lower 7 bits and set MSB to indicate more bytes
            value >>= 7; // Right shift by 7 bits
        }
        bytes.Add((byte)value); // Last byte, no MSB set
        return bytes.ToArray();
    }
}