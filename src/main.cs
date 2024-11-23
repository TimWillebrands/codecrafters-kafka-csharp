using System.Net;
using System.Net.Sockets;
using CodecraftersKafka;

Console.WriteLine("Logs from your program will appear here!");
var server = new TcpListener(IPAddress.Any, 9092);
var cancellationTokenSource = new CancellationTokenSource();

try
{
    server.Start();
    var socket = server.AcceptSocket();
    Console.WriteLine("Server started. Press Ctrl+C to stop.");

    var i = 1;

    Console.CancelKeyPress += (sender, e) =>
    {
        e.Cancel = true;
        cancellationTokenSource.Cancel();
    };

    while (!cancellationTokenSource.Token.IsCancellationRequested)
    {
        var buffer = new Span<byte>(new byte[1024]);
        socket.Receive(buffer);
        var request = KafkaRequest.FromSpan(buffer);

        Console.WriteLine($"{i} - Request: {request}");

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

        Console.WriteLine($"{i} - Response: {response}");

        var sendBytes = socket.Send(response.ToSpan());

        Console.WriteLine($"{i} - Response of '{sendBytes}' bytes was sent!");
        
        i++;
    }
}
finally
{
    server.Stop();
    server.Dispose();
    Console.WriteLine("Server stopped.");
}