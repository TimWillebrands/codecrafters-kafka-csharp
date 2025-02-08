using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;

namespace CodecraftersKafka;

internal record ClusterMetadataLog
{
    private const string PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    internal List<RecordBatch> Batches { get; } = [];

    internal ClusterMetadataLog()
    {
        var log
            = File.ReadAllBytes(PATH);

        var offset = 0;
        while (offset < log.Length)
        {
            Batches.Add(new RecordBatch(log, ref offset));
        }
    }

    internal readonly record struct RecordBatch(
        long BaseOffset,
        int BatchLength,
        int PartitionLeaderEpoch,
        byte MagicNumber,
        int Crc,
        short Attributes,
        int LastOffsetData,
        long BaseTimestamp,
        long MaxTimestamp,
        long ProducerId,
        short ProducerEpoch,
        int BaseSequence,
        int RecordsLength,
        List<Record> Records)
    {
        internal RecordBatch(ReadOnlyMemory<byte> bytes, ref int offset) : this(
            ReadInt64(bytes, ref offset),
            ReadInt32(bytes, ref offset),
            ReadInt32(bytes, ref offset),
            bytes.Span[offset++],
            ReadInt32(bytes, ref offset),
            ReadInt16(bytes, ref offset),
            ReadInt32(bytes, ref offset),
            ReadInt64(bytes, ref offset),
            ReadInt64(bytes, ref offset),
            ReadInt64(bytes, ref offset),
            ReadInt16(bytes, ref offset),
            ReadInt32(bytes, ref offset),
            ReadInt32(bytes, ref offset),
            []
        )
        {
            var sbytes = MemoryMarshal.Cast<byte, sbyte>(bytes.Span);
            for (var i = 0; i < RecordsLength; i++)
            {
                var len = VarintDecoder.ReadSignedVarint(sbytes, ref offset);
                var attr = bytes.Span[offset++];
                var tsDelta = VarintDecoder.ReadSignedVarint(sbytes, ref offset);
                var offDelta = VarintDecoder.ReadSignedVarint(sbytes, ref offset);
                var keyLen = VarintDecoder.ReadSignedVarint(sbytes, ref offset);
                var key = Array.Empty<byte>();
                var valueLen = VarintDecoder.ReadSignedVarint(sbytes, ref offset);
                var value = bytes[offset..(offset += valueLen)];
                var headerCnt = VarintDecoder.ReadUnsignedVarint(sbytes, ref offset);
                Records.Add(new Record(
                    len,
                    attr,
                    tsDelta,
                    offDelta,
                    keyLen,
                    key,
                    valueLen,
                    value,
                    headerCnt
                ));
            }
        }
    };

    internal readonly record struct Record(
        int Length,
        byte Attributes,
        int TimestampDelta,
        int OffsetDelta,
        int KeyLength,
        ReadOnlyMemory<byte> Key,
        int ValueLength,
        ReadOnlyMemory<byte> Value,
        int HeaderCnt)
    {
        internal RecordValue RecordValue => new RecordValue(Value);
    };

    internal readonly ref struct RecordValue(ReadOnlyMemory<byte> memory)
    {
        internal byte FrameVersion => memory.Span[0];
        internal RecordValueType Type => (RecordValueType) memory.Span[1];
        internal byte Version => memory.Span[2];

        internal (ReadOnlyMemory<byte> Name, short FeatureLength) FeatureLevel { get  {
            if(Type != RecordValueType.FeatureLevel) throw new InvalidOperationException();
            var (value, varIntLen) = VarintDecoder.ReadUnsignedVarint(memory.Span[3..]);
            var start = 4 + varIntLen;
            var end = start + value;
            Console.WriteLine(Encoding.UTF8.GetString(memory[start..end].Span));
            var offset = 0;
            return (
                memory[start..end], 
                ReadInt16(memory[end..], ref offset));
        }}

        internal (ReadOnlyMemory<byte> Name, ReadOnlyMemory<byte> Uuid) Topic { get  {
            if(Type != RecordValueType.Topic) throw new InvalidOperationException();
            var (v, varIntLen) = VarintDecoder.ReadUnsignedVarint(memory.Span[3..]);
            var value = v - 1;
            var start = 3 + varIntLen;
            var end = start + value;
            var name = memory[start..end];
            var uuid = memory[end..(end + 16)];
            return (name, uuid);
        }}

        internal PartitionRecord Partition { 
            get  {
                if(Type != RecordValueType.Partition) throw new InvalidOperationException();

                var offset = 3;
                var partitionId = ReadInt32(memory, ref offset);
                var uuid = memory[offset..(offset += 16)];
            
                var (repLen, repLenVarBytes) = VarintDecoder.ReadUnsignedVarint(memory.Span[offset..] );
                repLen -= 1;  // Compact Array so -1
                var replicaArr = memory[offset..(offset += repLen + repLenVarBytes)];
            
                var (syncLen, syncLenVarBytes) = VarintDecoder.ReadUnsignedVarint(memory.Span[offset..] );
                syncLen -= 1;
                var inSyncArr = memory[offset..(offset += syncLen + syncLenVarBytes)];
            
                var (remRepLen, remRepLenVarBytes) = VarintDecoder.ReadUnsignedVarint(memory.Span[offset..] );
                remRepLen -= 1;
                var remRepArr = memory[offset..(offset += remRepLen + remRepLenVarBytes)];
            
                var (addRepLen, addRepLenVarBytes) = VarintDecoder.ReadUnsignedVarint(memory.Span[offset..] );
                addRepLen -= 1;  // Compact Array so -1
                var addRepArr = memory[offset..(offset += addRepLen + addRepLenVarBytes)];
            
                return new PartitionRecord(
                    partitionId,
                    uuid,
                    ReadInt32(memory, ref offset),
                    ReadInt32(memory, ref offset),
                    replicaArr,
                    inSyncArr,
                    addRepArr,
                    remRepArr );
            }
        }
    }

    internal enum RecordValueType : byte
    {
        Topic = 2,
        Partition = 3,
        FeatureLevel = 12
    }

    internal readonly record struct PartitionRecord(
        int PartitionId,
        ReadOnlyMemory<byte> TopicUuid,
        int Leader,
        int LeaderEpoch,
        ReadOnlyMemory<byte> ReplicaNodes,
        ReadOnlyMemory<byte> InSync,
        ReadOnlyMemory<byte> AddingReplicas,
        ReadOnlyMemory<byte> RemovingReplicas );
    
    private static long ReadInt64(ReadOnlyMemory<byte> buffer, ref int offset)
        => BinaryPrimitives.ReadInt64BigEndian(buffer[offset..(offset += 8)].Span);

    private static int ReadInt32(ReadOnlyMemory<byte> buffer, ref int offset)
        => BinaryPrimitives.ReadInt32BigEndian(buffer[offset..(offset += 4)].Span);

    private static int ReadInt32(ReadOnlyMemory<byte> buffer, int offset)
        => BinaryPrimitives.ReadInt32BigEndian(buffer[offset..(offset + 4)].Span);

    private static short ReadInt16(ReadOnlyMemory<byte> buffer, ref int offset)
        => BinaryPrimitives.ReadInt16BigEndian(buffer[offset..(offset += 2)].Span);
}