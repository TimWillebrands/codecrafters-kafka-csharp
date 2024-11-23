using System.Net;
using System.Net.Sockets;
using CodecraftersKafka;

Console.WriteLine("Logs from your program will appear here!");
var server = new TcpListener(IPAddress.Any, 9092);
var cts = new CancellationTokenSource();

Console.CancelKeyPress += (sender, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    server.Start();
    Console.WriteLine("Server started. Press Ctrl+C to stop.");
    
    while (!cts.Token.IsCancellationRequested)
    {
        var socket = await server.AcceptSocketAsync(cts.Token);

        await Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var buffer = new ArraySegment<byte>(new byte[1024]);
                await socket.ReceiveAsync(buffer);
                var request = KafkaRequest.FromSpan(buffer);

                Console.WriteLine($"Request: {request}");

                var isApiVersionUnsupported = request.Header.ApiKeyVersion is <= 0 or > 4;

                var response = isApiVersionUnsupported
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
            }
        }).ContinueWith((t) =>
        {
            socket.Dispose();
        });
    }
}
finally
{
    server.Stop();
    server.Dispose();
    Console.WriteLine("Server stopped.");
}