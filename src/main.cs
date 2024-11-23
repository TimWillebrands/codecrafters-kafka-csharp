using System.Net;
using System.Net.Sockets;
using CodecraftersKafka;

Console.WriteLine("Logs from your program will appear here!");
var server = new TcpListener(IPAddress.Any, 9092);

server.Start();
var socket = server.AcceptSocket();
var buffer = new Span<byte>(new byte[1024]);
socket.Receive(buffer);
var request = KafkaRequest.FromSpan(buffer);

Console.WriteLine($"Request: {request}");

var isApiVersionSupported = request.Header.ApiKeyVersion is <= 0 or > 4;

var response = isApiVersionSupported 
    ? new KafkaResponse<ApiVersionsBody>(
        new ResponseHeader(request.Header.CorrelationId),
        new ApiVersionsBody(ErrorCode.UnsupportedVersion, []))
    : new KafkaResponse<ApiVersionsBody>(
        new ResponseHeader(request.Header.CorrelationId),
        new ApiVersionsBody(ErrorCode.None, [
            new ApiVersion(ApiKey.ApiVersions, 1, 4),
        ]));

Console.WriteLine($"Response: {response}");

var sendBytes = socket.Send(response.ToSpan());

Console.WriteLine($"Response of '{sendBytes}' bytes was sent!");

socket.Close();