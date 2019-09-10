package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	// You'll have to add definitions here.
	Operation string
	ID int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.

type CopyKeyValStoreArgs struct {
	SourceServer string
	KeyValStore map[string]string
	JobIDStore map[int64]int64
	LastJobID int64
}

type CopyKeyValStoreReply struct {
	Err Err
}

type ForwardBackupArgs struct {
	AppendArgs *PutAppendArgs
	LastJobID int64
}

type ForwardBackupReply struct {
	Err Err
}
