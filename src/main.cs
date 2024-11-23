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
        var client = await server.AcceptTcpClientAsync(cts.Token);
        _ = HandleClientAsync(client, cts.Token).ContinueWith(task =>
        {
            if (task.IsFaulted)
                Console.WriteLine($"Client error: {task.Exception}");
            client.Dispose();
        });
    }
}
finally
{
    server.Stop();
}
static async Task HandleClientAsync(TcpClient client, CancellationToken ct)
{
    await using var stream = client.GetStream();
    var buffer = new byte[1024];
    
    while (!ct.IsCancellationRequested)
    {
        var received = await stream.ReadAsync(buffer, ct);
        if (received == 0) break; // Client disconnected

        var request = KafkaRequest.FromSpan(new ArraySegment<byte>(buffer, 0, received));
        
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

        await stream.WriteAsync(response.ToSpan().ToArray(), ct);
    }
}