using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace CodecraftersKafka;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct KafkaRequest(int MessageSize, RequestHeader Header)
{
    public ReadOnlySpan<byte> ToSpan()
    {
        var span = new Span<byte>(new byte[Marshal.SizeOf<KafkaResponse>()]);

        BinaryPrimitives.WriteInt32BigEndian(span[..4], MessageSize);
        BinaryPrimitives.WriteInt32BigEndian(span[4..8], Header.CorrelationId);

        return span;
    }
    
    public static KafkaRequest FromSpan(ReadOnlySpan<byte> span)
    {
        if (span.Length < Marshal.SizeOf<KafkaRequest>())
            throw new ArgumentException("Span is too small for KafkaResponse.");

        var messageSize = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var apiKey = BinaryPrimitives.ReadInt16BigEndian(span[4..6]);
        var apiKeyVersion = BinaryPrimitives.ReadInt16BigEndian(span[6..8]);
        var correlationId = BinaryPrimitives.ReadInt32BigEndian(span[8..16]);

        return new KafkaRequest(messageSize, new RequestHeader(apiKey, apiKeyVersion, correlationId)); 
    }
};

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct RequestHeader(short ApiKey, short ApiKeyVersion, int CorrelationId);