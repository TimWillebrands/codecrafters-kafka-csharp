using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

Console.WriteLine("Logs from your program will appear here!");
var server = new TcpListener(IPAddress.Any, 9092);

server.Start();
var socket = server.AcceptSocket();
var buffer = new Span<byte>(new byte[1024]);
var bytesRead = socket.Receive(buffer);
var request = KafkaResponse.FromSpan(buffer);
// var reader = new BinaryReader(new MemoryStream(buffer, 0, bytesRead));
// var messageSize = reader.ReadInt32();
// var correlationId = reader.ReadInt32();
Console.WriteLine($"request: {request}");

var response = new KafkaResponse(0, new Header(7));
Console.WriteLine($"response: {response}");

socket.Send(response.ToSpan());

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct KafkaResponse(int MessageSize, Header Header)
{
    public ReadOnlySpan<byte> ToSpan()
    {
        var span = new Span<byte>(new byte[Marshal.SizeOf<KafkaResponse>()]);

        BinaryPrimitives.WriteInt32BigEndian(span[..4], MessageSize);
        BinaryPrimitives.WriteInt32BigEndian(span[4..8], Header.CorrelationId);

        return span;
    }
    
    public static KafkaResponse FromSpan(ReadOnlySpan<byte> span)
    {
        if (span.Length < Marshal.SizeOf<KafkaResponse>())
            throw new ArgumentException("Span is too small for KafkaResponse.");

        var messageSize = BinaryPrimitives.ReadInt32BigEndian(span[..4]);
        var correlationId = BinaryPrimitives.ReadInt32BigEndian(span[4..8]);

        return new KafkaResponse(messageSize, new Header(correlationId)); 
    }
};

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct Header(int CorrelationId);