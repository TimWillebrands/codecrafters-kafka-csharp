using System.Net;
using System.Net.Sockets;
using CodecraftersKafka;

Console.WriteLine("Logs from your program will appear here!");
var server = new TcpListener(IPAddress.Any, 9092);

server.Start();
var socket = server.AcceptSocket();
var buffer = new Span<byte>(new byte[1024]);
var bytesRead = socket.Receive(buffer);
var request = KafkaRequest.FromSpan(buffer);
// var reader = new BinaryReader(new MemoryStream(buffer, 0, bytesRead));
// var messageSize = reader.ReadInt32();
// var correlationId = reader.ReadInt32();
Console.WriteLine($"request: {request}");

var response = new KafkaResponse<ApiVersionsBody>(
    0, 
    new ResponseHeader(request.Header.CorrelationId), 
    new ApiVersionsBody(ErrorCode.UnsupportedVersion));

Console.WriteLine($"response: {response}");

socket.Send(response.ToSpan());
