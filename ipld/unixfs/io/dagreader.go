package io

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/go-cid"
	"io"
	"os"
	"sync"
	"time"

	mdag "github.com/ipfs/boxo/ipld/merkledag"
	unixfs "github.com/ipfs/boxo/ipld/unixfs"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/klauspost/reedsolomon"
)

// Common errors
var (
	ErrIsDir            = errors.New("this dag node is a directory")
	ErrCantReadSymlinks = errors.New("cannot currently read symlinks")
	ErrUnkownNodeType   = errors.New("unknown node type")
	ErrSeekNotSupported = errors.New("file does not support seeking")
)

// TODO: Rename the `DagReader` interface, this doesn't read *any* DAG, just
// DAGs with UnixFS node (and it *belongs* to the `unixfs` package). Some
// alternatives: `FileReader`, `UnixFSFileReader`, `UnixFSReader`.

// A DagReader provides read-only read and seek acess to a unixfs file.
// Different implementations of readers are used for the different
// types of unixfs/protobuf-encoded nodes.
type DagReader interface {
	ReadSeekCloser
	Size() uint64
	Mode() os.FileMode
	ModTime() time.Time
	CtxReadFull(context.Context, []byte) (int, error)
}

// A ReadSeekCloser implements interfaces to read, copy, seek and close.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
	io.WriterTo
}

// NewDagReader creates a new reader object that reads the data represented by
// the given node, using the passed in DAGService for data retrieval.
func NewDagReader(ctx context.Context, n ipld.Node, serv ipld.NodeGetter) (DagReader, error) {
	var size uint64
	var mode os.FileMode
	var modTime time.Time

	switch n := n.(type) {
	case *mdag.RawNode:
		size = uint64(len(n.RawData()))

	case *mdag.ProtoNode:
		fsNode, err := unixfs.FSNodeFromBytes(n.Data())
		if err != nil {
			return nil, err
		}

		mode = fsNode.Mode()
		modTime = fsNode.ModTime()

		switch fsNode.Type() {
		case unixfs.TFile, unixfs.TRaw:
			size = fsNode.FileSize()

		case unixfs.TDirectory, unixfs.THAMTShard:
			// Dont allow reading directories
			return nil, ErrIsDir

		case unixfs.TMetadata:
			if len(n.Links()) == 0 {
				return nil, errors.New("incorrectly formatted metadata object")
			}
			child, err := n.Links()[0].GetNode(ctx, serv)
			if err != nil {
				return nil, err
			}

			childpb, ok := child.(*mdag.ProtoNode)
			if !ok {
				return nil, mdag.ErrNotProtobuf
			}
			return NewDagReader(ctx, childpb, serv)
		case unixfs.TSymlink:
			return nil, ErrCantReadSymlinks
		default:
			return nil, unixfs.ErrUnrecognizedType
		}
	default:
		return nil, ErrUnkownNodeType
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)

	return &dagReader{
		ctx:       ctxWithCancel,
		cancel:    cancel,
		serv:      serv,
		size:      size,
		mode:      mode,
		modTime:   modTime,
		rootNode:  n,
		dagWalker: ipld.NewWalker(ctxWithCancel, ipld.NewNavigableIPLDNode(n, serv)),
		mechanism: "Rep",
	}, nil
}

// AltReader creates a new reader object that reads the data represented by
// the given node, using the passed in DAGService for data retrieval  with EC option.
func AltReader(ctx context.Context, n ipld.Node, serv ipld.NodeGetter, or int, par int, chunksize uint64, mechanism string, interval float64) (DagReader, error) {
	var size uint64
	var mode os.FileMode
	var modTime time.Time

	switch n := n.(type) {
	case *mdag.RawNode:
		size = uint64(len(n.RawData()))

	case *mdag.ProtoNode:
		fsNode, err := unixfs.FSNodeFromBytes(n.Data())
		if err != nil {
			return nil, err
		}

		mode = fsNode.Mode()
		modTime = fsNode.ModTime()

		switch fsNode.Type() {
		case unixfs.TFile, unixfs.TRaw:
			size = fsNode.FileSize()

		case unixfs.TDirectory, unixfs.THAMTShard:
			// Dont allow reading directories
			return nil, ErrIsDir

		case unixfs.TMetadata:
			if len(n.Links()) == 0 {
				return nil, errors.New("incorrectly formatted metadata object")
			}
			child, err := n.Links()[0].GetNode(ctx, serv)
			if err != nil {
				return nil, err
			}

			childpb, ok := child.(*mdag.ProtoNode)
			if !ok {
				return nil, mdag.ErrNotProtobuf
			}
			return AltReader(ctx, childpb, serv, or, par, chunksize, mechanism, interval)
		case unixfs.TSymlink:
			return nil, ErrCantReadSymlinks
		default:
			return nil, unixfs.ErrUnrecognizedType
		}
	default:
		return nil, ErrUnkownNodeType
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	return &dagReader{
		ctx:         ctxWithCancel,
		cancel:      cancel,
		serv:        serv,
		size:        size,
		mode:        mode,
		modTime:     modTime,
		rootNode:    n,
		dagWalker:   ipld.NewWalker(ctxWithCancel, ipld.NewNavigableIPLDNode(n, serv)),
		mechanism:   mechanism,
		or:          or,
		par:         par,
		chunksize:   chunksize,
		nodesToExtr: make([]ipld.Node, 0),
		times:       make([]time.Duration, 0),
		Indexes:     make([]int, 0),
		startOfNext: 0,
		toskip:      true,
		written:     0,
		retnext:     make([]linkswithindexes, 0),
		stop:        false,
		interval:    interval,
	}, nil
}

// dagReader provides a way to easily read the data contained in a dag.
type dagReader struct {
	// Structure to perform the DAG iteration and search, the reader
	// just needs to add logic to the `Visitor` callback passed to
	// `Iterate` and `Seek`.
	dagWalker *ipld.Walker

	// Buffer with the data extracted from the current node being visited.
	// To avoid revisiting a node to complete a (potential) partial read
	// (or read after seek) the node's data is fully extracted in a single
	// `readNodeDataBuffer` operation.
	currentNodeData *bytes.Reader

	// Implements the `Size()` API.
	size uint64

	// Current offset for the read head within the DAG file.
	offset int64

	// Root node of the DAG, stored to re-create the `dagWalker` (effectively
	// re-setting the position of the reader, used during `Seek`).
	rootNode ipld.Node

	// Context passed to the `dagWalker`, the `cancel` function is used to
	// cancel read operations (cancelling requested child node promises,
	// see `ipld.NavigableIPLDNode.FetchChild` for details).
	ctx    context.Context
	cancel func()

	// Passed to the `dagWalker` that will use it to request nodes.
	// TODO: Revisit name.
	serv             ipld.NodeGetter
	mode             os.FileMode
	modTime          time.Time
	or               int
	par              int
	chunksize        uint64
	mechanism        string
	nodesToExtr      []ipld.Node
	mu               sync.Mutex
	wg               sync.WaitGroup
	recnostructtimes int
	timetakenDecode  time.Duration
	verificationTime time.Duration
	times            []time.Duration
	Indexes          []int
	startOfNext      int
	muworker         sync.Mutex
	toskip           bool
	written          uint64
	retnext          []linkswithindexes
	stop             bool
	interval         float64
}

// Mode returns the UnixFS file mode or 0 if not set.
func (dr *dagReader) Mode() os.FileMode {
	return dr.mode
}

// ModTime returns the UnixFS file last modification time if set.
func (dr *dagReader) ModTime() time.Time {
	return dr.modTime
}

// Size returns the total size of the data from the DAG structured file.
func (dr *dagReader) Size() uint64 {
	return dr.size
}

// Read implements the `io.Reader` interface through the `CtxReadFull`
// method using the DAG reader's internal context.
func (dr *dagReader) Read(b []byte) (int, error) {
	return dr.CtxReadFull(dr.ctx, b)
}

// CtxReadFull reads data from the DAG structured file. It always
// attempts a full read of the DAG until the `out` buffer is full.
// It uses the `Walker` structure to iterate the file DAG and read
// every node's data into the `out` buffer.
func (dr *dagReader) CtxReadFull(ctx context.Context, out []byte) (n int, err error) {
	// Set the `dagWalker`'s context to the `ctx` argument, it will be used
	// to fetch the child node promises (see
	// `ipld.NavigableIPLDNode.FetchChild` for details).
	dr.dagWalker.SetContext(ctx)

	// If there was a partially read buffer from the last visited
	// node read it before visiting a new one.
	if dr.currentNodeData != nil {
		// TODO: Move this check inside `readNodeDataBuffer`?
		n = dr.readNodeDataBuffer(out)

		if n == len(out) {
			return n, nil
			// Output buffer full, no need to traverse the DAG.
		}
	}

	// Iterate the DAG calling the passed `Visitor` function on every node
	// to read its data into the `out` buffer, stop if there is an error or
	// if the entire DAG is traversed (`EndOfDag`).
	err = dr.dagWalker.Iterate(func(visitedNode ipld.NavigableNode) error {
		node := ipld.ExtractIPLDNode(visitedNode)

		// Skip internal nodes, they shouldn't have any file data
		// (see the `balanced` package for more details).
		if len(node.Links()) > 0 {
			return nil
		}

		err = dr.saveNodeData(node)
		if err != nil {
			return err
		}
		// Save the leaf node file data in a buffer in case it is only
		// partially read now and future `CtxReadFull` calls reclaim the
		// rest (as each node is visited only once during `Iterate`).
		//
		// TODO: We could check if the entire node's data can fit in the
		// remaining `out` buffer free space to skip this intermediary step.

		n += dr.readNodeDataBuffer(out[n:])

		if n == len(out) {
			// Output buffer full, no need to keep traversing the DAG,
			// signal the `Walker` to pause the iteration.
			dr.dagWalker.Pause()
		}

		return nil
	})

	if err == ipld.EndOfDag {
		return n, io.EOF
		// Reached the end of the (DAG) file, no more data to read.
	} else if err != nil {
		return n, err
		// Pass along any other errors from the `Visitor`.
	}

	return n, nil
}

// Save the UnixFS `node`'s data into the internal `currentNodeData` buffer to
// later move it to the output buffer (`Read`) or seek into it (`Seek`).
func (dr *dagReader) saveNodeData(node ipld.Node) error {
	extractedNodeData, err := unixfs.ReadUnixFSNodeData(node)
	if err != nil {
		return err
	}

	dr.currentNodeData = bytes.NewReader(extractedNodeData)
	return nil
}

// Read the `currentNodeData` buffer into `out`. This function can't have
// any errors as it's always reading from a `bytes.Reader` and asking only
// the available data in it.
func (dr *dagReader) readNodeDataBuffer(out []byte) int {
	n, _ := dr.currentNodeData.Read(out)
	// Ignore the error as the EOF may not be returned in the first
	// `Read` call, explicitly ask for an empty buffer below to check
	// if we've reached the end.

	if dr.currentNodeData.Len() == 0 {
		dr.currentNodeData = nil
		// Signal that the buffer was consumed (for later `Read` calls).
		// This shouldn't return an EOF error as it's just the end of a
		// single node's data, not the entire DAG.
	}

	dr.offset += int64(n)
	// TODO: Should `offset` be incremented here or in the calling function?
	// (Doing it here saves LoC but may be confusing as it's more hidden).

	return n
}

// Similar to `readNodeDataBuffer` but it writes the contents to
// an `io.Writer` argument.
//
// TODO: Check what part of the logic between the two functions
// can be extracted away.
func (dr *dagReader) writeNodeDataBuffer(w io.Writer) (int64, error) {
	n, err := dr.currentNodeData.WriteTo(w)
	if err != nil {
		return n, err
	}

	if dr.currentNodeData.Len() == 0 {
		dr.currentNodeData = nil
		// Signal that the buffer was consumed (for later `Read` calls).
		// This shouldn't return an EOF error as it's just the end of a
		// single node's data, not the entire DAG.
	}

	dr.offset += n
	return n, nil
}

// WriteTo writes to the given writer.
// This follows the `bytes.Reader.WriteTo` implementation
// where it starts from the internal index that may have
// been modified by other `Read` calls.
//
// TODO: This implementation is very similar to `CtxReadFull`,
// the common parts should be abstracted away.
func (dr *dagReader) WriteTo(w io.Writer) (n int64, err error) {
	if dr.mechanism == "Rep" {
		return dr.READREP(w)
	} else {
		return dr.READEC(w)
	}
}

func (dr *dagReader) READEC(w io.Writer) (n int64, err error) {
	// Use the internal reader's context to fetch the child node promises
	// (see `ipld.NavigableIPLDNode.FetchChild` for details).
	dr.dagWalker.SetContext(dr.ctx)

	// If there was a partially read buffer from the last visited
	// node read it before visiting a new one.
	if dr.currentNodeData != nil {
		n, err = dr.writeNodeDataBuffer(w)
		if err != nil {
			return n, err
		}
	}

	// Iterate the DAG calling the passed `Visitor` function on every node
	// to read its data into the `out` buffer, stop if there is an error or
	// if the entire DAG is traversed (`EndOfDag`).
	start := time.Now()
	err = dr.dagWalker.ECIterate(func(visitedNode ipld.NavigableNode) error {
		node := ipld.ExtractIPLDNode(visitedNode)
		dr.nodesToExtr = append(dr.nodesToExtr, node)
		return nil
	}, uint64(dr.chunksize))

	if err == ipld.EndOfDag {
		end := time.Now()
		fmt.Fprintf(os.Stdout, "Time taken to get the internal nodes on the level before the last one : %s \n", end.Sub(start).String())
		if dr.mechanism == "exactN" {
			/*dr.WriteN(w)
			fmt.Fprintf(os.Stdout, "Time taken to reconstruct nodes : %s \n", dr.timetakenDecode.String())
			fmt.Fprintf(os.Stdout, "Time taken for verification : %s \n", dr.verificationTime.String())
			*/return 0, nil
		}
		if dr.mechanism == "allN" {
			dr.WriteNPlusK(w)
			fmt.Fprintf(os.Stdout, "Nb of nodes : %d \n", dr.recnostructtimes)
			fmt.Fprintf(os.Stdout, "Time taken to reconstruct nodes : %s \n", dr.timetakenDecode.String())
			fmt.Fprintf(os.Stdout, "Time taken for verification : %s \n", dr.verificationTime.String())
			return 0, nil
		}
		if dr.mechanism == "originalN" {
			dr.WriteNOriginal(w)
			//fmt.Fprintf(os.Stdout, "Time taken to reconstruct nodes : %s \n", dr.timetakenDecode.String())
			//fmt.Fprintf(os.Stdout, "Time taken for verification : %s \n", dr.verificationTime.String())
			return 0, nil
		}
		if dr.mechanism == "ECWID" {
			//dr.WriteNWID(w)
			dr.WriteNWID5(w)
			//fmt.Fprintf(os.Stdout, "Time taken to reconstruct nodes : %s \n", dr.timetakenDecode.String())
			//fmt.Fprintf(os.Stdout, "Time taken for verification : %s \n", dr.verificationTime.String())
			return 0, nil
		}
		if dr.mechanism == "Cont" {
			dr.WriteCont(w)
			//fmt.Fprintf(os.Stdout, "Time taken to reconstruct nodes : %s \n", dr.timetakenDecode.String())
			//fmt.Fprintf(os.Stdout, "Time taken for verification : %s \n", dr.verificationTime.String())
			return 0, nil
		}
		if dr.mechanism == "ECWIDPlusOne" {
			//dr.WriteNWIDPlusOne(w)
			//fmt.Fprintf(os.Stdout, "Time taken to reconstruct nodes : %s \n", dr.timetakenDecode.String())
			//fmt.Fprintf(os.Stdout, "Time taken for verification : %s \n", dr.verificationTime.String())
			return 0, nil
		}
		if dr.mechanism == "originalplus1" {
			//dr.WriteNPlusOne(w)
			//fmt.Fprintf(os.Stdout, "Time taken to reconstruct nodes : %s \n", dr.timetakenDecode.String())
			//fmt.Fprintf(os.Stdout, "Time taken for verification : %s \n", dr.verificationTime.String())
			return 0, nil
		}
		if dr.mechanism == "originalplus2" {
			//dr.WriteNPlusTwo(w)
			//fmt.Fprintf(os.Stdout, "Time taken to reconstruct nodes : %s \n", dr.timetakenDecode.String())
			//fmt.Fprintf(os.Stdout, "Time taken for verification : %s \n", dr.verificationTime.String())
			return 0, nil
		}
	}

	return n, err

}

// //////////////////// Downloading only the original data /////////////////////
func (dr *dagReader) WriteNOriginal(w io.Writer) (err error) {
	skipped := 0
	var writetime time.Duration
	var downloadtime time.Duration
	var written uint64
	written = 0
	nbr := 0
	fmt.Fprintf(os.Stdout, "OOOOOOOOOMMMMMMMMMMMNNNNNNNNNNNNNNNN  \n")
	for _, n := range dr.nodesToExtr {
		fmt.Fprintf(os.Stdout, "Length of nodes to extract is : %d and length of links is: %d  \n", len(dr.nodesToExtr),len(n.Links()))
		for _, l := range n.Links() {
			if len(dr.retnext) < dr.or {
				topass := linkswithindexes{Link: l, Index: nbr % (dr.or + dr.par)}
				dr.retnext = append(dr.retnext, topass)
			} else {
				if len(dr.retnext) == dr.or {
					skipped++
					if skipped == dr.par {
						wrote := 0
						//countchecked = 0
						//open channel with context
						ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
						//start n+k gourotines and start retrieving parallel nodes
						dr.wg.Add(dr.or)
						d := time.Now()
						togetmany := make([]cid.Cid, 0)
						//fmt.Fprintf(os.Stdout, "Start download the linksparallel %s  \n", time.Now().Format("15:04:05.000"))
						// Create a map of CID -> Index from linksparallel
cidIndexMap := make(map[cid.Cid][]int)
for _, ci := range dr.retnext {
    togetmany = append(togetmany, ci.Link.Cid)
    cidIndexMap[ci.Link.Cid] = append(cidIndexMap[ci.Link.Cid], ci.Index)
}

// Launch GetMany
chann := dr.serv.GetMany(ctx, togetmany)
shards := make([][]byte, dr.or+dr.par)

// Read from channel
for value := range chann {

    indexes, ok := cidIndexMap[value.Node.Cid()]
    if !ok {
        // Should not happen unless GetMany returns unexpected CIDs
        continue
    }

    // One CID might correspond to multiple indexes
    for _, idx := range indexes {
		if idx < dr.or {
		wrote++
        fmt.Fprintf(os.Stdout, "69 index : %d  \n", idx)
        shards[idx], _ = unixfs.ReadUnixFSNodeData(value.Node)
        dr.wg.Done()
		if wrote >= dr.or{
			break
		}
    }
	}
	 if wrote >= dr.or {
        cancel()
        break
    }
		
}

// Wait
dr.wg.Wait()
						downloadtime += time.Since(d)
						//fmt.Fprintf(os.Stdout, "Finished reading from channel and updating indexes and reading to shards nad start reconstruction and verification retnext %s  \n", time.Now().Format("15:04:05.000"))
						//fmt.Fprintf(os.Stdout, "Finished reconstruction and verification and start writing retnext %s  \n", time.Now().Format("15:04:05.000"))
						for i, shard := range shards {
							if i < dr.or {
								if written+uint64(len(shard)) <= dr.size {
									dd := time.Now()
									w.Write(shard)
									written += uint64(len(shard))
									writetime += time.Since(dd)
								} else {
									ddd := time.Now()
									towrite := shard[0 : dr.size-written]
									w.Write(towrite)
									writetime += time.Since(ddd)
									fmt.Fprintf(os.Stdout, "New log write time is : %s  \n", writetime.String())
									fmt.Fprintf(os.Stdout, "New log download time is : %s  \n", downloadtime.String())
									return nil
								}
							}
						}
						dr.retnext = make([]linkswithindexes, 0)
						skipped = 0
					}

				}
			}
			nbr++
		}
	}
	fmt.Fprintf(os.Stdout, "New log write time is : %s  \n", writetime.String())
	fmt.Fprintf(os.Stdout, "New log download time is : %s  \n", downloadtime.String())
	return nil
}

// /////////////// Downloading all chunks and the fastest N chunks we retrieve we will be writing it to disk /////////////////////////
func (dr *dagReader) WriteNPlusK(w io.Writer) (err error) {
	fmt.Fprintf(os.Stdout, "Start function  %s  \n", time.Now().Format("15:04:05.000"))
	var writetime time.Duration
	var reconstructiontime time.Duration
	var downloadtimesixnine time.Duration
	sixninetime := 0
	nbver := 0
	nbr := 0
	sixnine := false  
	enc, _ := reedsolomon.New(dr.or, dr.par)
	var written uint64
	written = 0
	fmt.Fprintf(os.Stdout, "The length of internal nodes are : %d  \n", len(dr.nodesToExtr))
	for _, n := range dr.nodesToExtr {
		fmt.Fprintf(os.Stdout, "There are %d links in this node  \n", len(n.Links()))
		for _, l := range n.Links() {
			if len(dr.retnext) < dr.or+dr.par {
				topass := linkswithindexes{Link: l, Index: nbr % (dr.or + dr.par)}
				dr.retnext = append(dr.retnext, topass)
				//fmt.Fprintf(os.Stdout, "Filled 1 in the retnext %s  \n", time.Now().Format("15:04:05.000"))
			}
			if len(dr.retnext) == dr.or+dr.par {
				sixnine = true
				//fmt.Fprintf(os.Stdout, "Finish filling the retnext %s  \n", time.Now().Format("15:04:05.000"))
			}
			if sixnine {
				wrote := 0
				//open channel with context
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
				//start n+k gourotines and start retrieving parallel nodes
				dr.wg.Add(dr.or)
				togetmany := make([]cid.Cid, 0)
				d := time.Now()
				//fmt.Fprintf(os.Stdout, "Start download the linksparallel %s  \n", time.Now().Format("15:04:05.000"))
				// Create a map of CID -> Index from linksparallel
				// Build map: cid -> list of indexes
cidIndexMap := make(map[cid.Cid][]int)
for _, ci := range dr.retnext {
    togetmany = append(togetmany, ci.Link.Cid)
    cidIndexMap[ci.Link.Cid] = append(cidIndexMap[ci.Link.Cid], ci.Index)
}

// Launch GetMany
chann := dr.serv.GetMany(ctx, togetmany)
shards := make([][]byte, dr.or+dr.par)
reconstruct := 0

// Read from channel
for value := range chann {

    indexes, ok := cidIndexMap[value.Node.Cid()]
    if !ok {
        // Should not happen unless GetMany returns unexpected CIDs
        continue
    }

    // One CID might correspond to multiple indexes
    for _, idx := range indexes {
		 wrote++
        fmt.Fprintf(os.Stdout, "69 index : %d  \n", idx)
        shards[idx], _ = unixfs.ReadUnixFSNodeData(value.Node)
        if idx >= dr.or {
            reconstruct = 1
        }
        dr.wg.Done()
		if wrote >= dr.or {
			break
		}
    }

    if wrote >= dr.or {
        cancel()
        break
    }
}

// Wait
dr.wg.Wait()
				//time.Sleep(100 * time.Millisecond)
				downloadtimesixnine += time.Since(d)
				sixninetime++
				//fmt.Fprintf(os.Stdout, "Finished reading from channel and updating indexes and reading to shards nad start reconstruction and verification retnext %s  \n", time.Now().Format("15:04:05.000"))
				if reconstruct == 1 {
					nbver++
					sss1 := time.Now()
					dr.recnostructtimes++
					start := time.Now()
					enc.Reconstruct(shards)
					end := time.Now()
					dr.timetakenDecode += end.Sub(start)
					st := time.Now()
					enc.Verify(shards)
					en := time.Now()
					dr.verificationTime += en.Sub(st)
					reconstructiontime += time.Since(sss1)
				}
				//fmt.Fprintf(os.Stdout, "Finished reconstruction and verification and start writing retnext %s  \n", time.Now().Format("15:04:05.000"))
				for i, shard := range shards {
					if i < dr.or {
						if written+uint64(len(shard)) <= dr.size {
							wr1 := time.Now()
							w.Write(shard)
							written += uint64(len(shard))
							writetime += time.Since(wr1)
						} else {
							wr1 := time.Now()
							towrite := shard[0 : dr.size-written]
							w.Write(towrite)
							writetime += time.Since(wr1)
							fmt.Fprintf(os.Stdout, "New log write time is : %s  \n", writetime.String())
							fmt.Fprintf(os.Stdout, "New log reconstruction and verification time is : %s  \n", reconstructiontime.String())
							fmt.Fprintf(os.Stdout, "New log download time is : %s  \n", downloadtimesixnine.String())
							fmt.Fprintf(os.Stdout, "New log number of reconstructions is : %d  \n", nbver)
							return nil
						}
					}
				}
				///	fmt.Fprintf(os.Stdout, "Finished writing retnext %s  \n", time.Now().Format("15:04:05.000"))
				dr.retnext = make([]linkswithindexes, 0)
				sixnine = false
			}
			nbr++
		}
	}
	fmt.Fprintf(os.Stdout, "New log write time is : %s  \n", writetime.String())
	fmt.Fprintf(os.Stdout, "New log reconstruction and verification time is : %s  \n", reconstructiontime.String())
	fmt.Fprintf(os.Stdout, "New log download time is : %s  \n", downloadtimesixnine.String())
	fmt.Fprintf(os.Stdout, "New log number of reconstructions is : %d  \n", nbver)

	return nil
}

func contains(slice []int, value int) bool {
	for _, v := range slice {
		if v == value {
			fmt.Fprintf(os.Stdout, "!!!!!!! %d is in the interval !!!!!!!!! \n", value)
			return true
		}
	}
	fmt.Fprintf(os.Stdout, "!!!!!!! %d is not in the interval !!!!!!!!! \n", value)
	return false
}

func (dr *dagReader) READREP(w io.Writer) (n int64, err error) {
	// Use the internal reader's context to fetch the child node promises
	// (see `ipld.NavigableIPLDNode.FetchChild` for details).
	dr.dagWalker.SetContext(dr.ctx)

	// If there was a partially read buffer from the last visited
	// node read it before visiting a new one.
	if dr.currentNodeData != nil {
		n, err = dr.writeNodeDataBuffer(w)
		if err != nil {
			return n, err
		}
	}

	// Iterate the DAG calling the passed `Visitor` function on every node
	// to read its data into the `out` buffer, stop if there is an error or
	// if the entire DAG is traversed (`EndOfDag`).
	err = dr.dagWalker.Iterate(func(visitedNode ipld.NavigableNode) error {
		node := ipld.ExtractIPLDNode(visitedNode)

		// Skip internal nodes, they shouldn't have any file data
		// (see the `balanced` package for more details).
		if len(node.Links()) > 0 {
			return nil
		}
		fmt.Fprintf(os.Stdout, "END of downloading the chunk : %s \n", time.Now().Format("2006-01-02 15:04:05.000"))

		err = dr.saveNodeData(node)
		if err != nil {
			return err
		}
		// Save the leaf node file data in a buffer in case it is only
		// partially read now and future `CtxReadFull` calls reclaim the
		// rest (as each node is visited only once during `Iterate`).
		written, err := dr.writeNodeDataBuffer(w)
		n += written
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "Start to download the chunk : %s \n", time.Now().Format("2006-01-02 15:04:05.000"))

		return nil
	})
	if err == ipld.EndOfDag {
		return n, nil
	}

	return n, err
}

// Close the reader (cancelling fetch node operations requested with
// the internal context, that is, `Read` calls but not `CtxReadFull`
// with user-supplied contexts).
func (dr *dagReader) Close() error {
	dr.cancel()
	return nil
}

// Seek implements `io.Seeker` seeking to a given offset in the DAG file,
// it matches the standard unix `seek`. It moves the position of the internal
// `dagWalker` and may also leave a `currentNodeData` buffer loaded in case
// the seek is performed to the middle of the data in a node.
//
// TODO: Support seeking from the current position (relative seek)
// through the `dagWalker` in `io.SeekCurrent`.
func (dr *dagReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return dr.offset, errors.New("invalid offset")
		}

		if offset == dr.offset {
			return offset, nil
			// Already at the requested `offset`, nothing to do.
		}

		left := offset
		// Amount left to seek.

		// Seek from the beginning of the DAG.
		dr.resetPosition()

		// Shortcut seeking to the beginning, we're already there.
		if offset == 0 {
			return 0, nil
		}

		// Use the internal reader's context to fetch the child node promises
		// (see `ipld.NavigableIPLDNode.FetchChild` for details).
		dr.dagWalker.SetContext(dr.ctx)
		// TODO: Performance: we could adjust here `preloadSize` of
		// `ipld.NavigableIPLDNode` also, when seeking we only want
		// to fetch one child at a time.

		// Seek the DAG by calling the provided `Visitor` function on every
		// node the `dagWalker` descends to while searching which can be
		// either an internal or leaf node. In the internal node case, check
		// the child node sizes and set the corresponding child index to go
		// down to next. In the leaf case (last visit of the search), if there
		// is still an amount `left` to seek do it inside the node's data
		// saved in the `currentNodeData` buffer, leaving it ready for a `Read`
		// call.
		err := dr.dagWalker.Seek(func(visitedNode ipld.NavigableNode) error {
			node := ipld.ExtractIPLDNode(visitedNode)

			if len(node.Links()) > 0 {
				// Internal node, should be a `mdag.ProtoNode` containing a
				// `unixfs.FSNode` (see the `balanced` package for more details).
				fsNode, err := unixfs.ExtractFSNode(node)
				if err != nil {
					return err
				}

				// If there aren't enough size hints don't seek
				// (see the `io.EOF` handling error comment below).
				if fsNode.NumChildren() != len(node.Links()) {
					return ErrSeekNotSupported
				}

				// Internal nodes have no data, so just iterate through the
				// sizes of its children (advancing the child index of the
				// `dagWalker`) to find where we need to go down to next in
				// the search.
				for {
					childSize := fsNode.BlockSize(int(dr.dagWalker.ActiveChildIndex()))

					if childSize > uint64(left) {
						// This child's data contains the position requested
						// in `offset`, go down this child.
						return nil
					}

					// Else, skip this child.
					left -= int64(childSize)
					err := dr.dagWalker.NextChild()
					if err == ipld.ErrNextNoChild {
						// No more child nodes available, nothing to do,
						// the `Seek` will stop on its own.
						return nil
					} else if err != nil {
						return err
						// Pass along any other errors (that may in future
						// implementations be returned by `Next`) to stop
						// the search.
					}
				}

			} else {
				// Leaf node, seek inside its data.
				err := dr.saveNodeData(node)
				if err != nil {
					return err
				}

				_, err = dr.currentNodeData.Seek(left, io.SeekStart)
				if err != nil {
					return err
				}
				// The corner case of a DAG consisting only of a single (leaf)
				// node should make no difference here. In that case, where the
				// node doesn't have a parent UnixFS node with size hints, this
				// implementation would allow this `Seek` to be called with an
				// argument larger than the buffer size which normally wouldn't
				// happen (because we would skip the node based on the size
				// hint) but that would just mean that a future `CtxReadFull`
				// call would read no data from the `currentNodeData` buffer.
				// TODO: Re-check this reasoning.

				return nil
				// In the leaf node case the search will stop here.
			}
		})
		if err != nil {
			return 0, err
		}

		dr.offset = offset
		return dr.offset, nil

	case io.SeekCurrent:
		if offset == 0 {
			return dr.offset, nil
		}

		return dr.Seek(dr.offset+offset, io.SeekStart)
		// TODO: Performance. This can be improved supporting relative
		// searches in the `Walker` (see `Walker.Seek`).

	case io.SeekEnd:
		return dr.Seek(int64(dr.Size())+offset, io.SeekStart)

	default:
		return 0, errors.New("invalid whence")
	}
}

// Reset the reader position by resetting the `dagWalker` and discarding
// any partially used node's data in the `currentNodeData` buffer, used
// in the `SeekStart` case.
func (dr *dagReader) resetPosition() {
	dr.currentNodeData = nil
	dr.offset = 0

	dr.dagWalker = ipld.NewWalker(dr.ctx, ipld.NewNavigableIPLDNode(dr.rootNode, dr.serv))
	// TODO: This could be avoided (along with storing the `dr.rootNode` and
	// `dr.serv` just for this call) if `Reset` is supported in the `Walker`.
}

type nodeswithindexes struct {
	Node  ipld.Node
	Index int
}

type nodeswithindexeswithtime struct {
	Node  ipld.Node
	Index int
	t     time.Duration
}

type linkswithindexes struct {
	Link  *ipld.Link
	Index int
}

////////////////////////////
/////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////////////
/////////////////////////////////////////////////////////

func (dr *dagReader) WriteNWID4(w io.Writer) error {
	ctxx, cancell := context.WithCancel(context.Background())
	nextready := make(chan struct{}, 1)
	indexesready := make(chan struct{}, 1)
	go dr.startTimerNew4(ctxx, w, cancell, nextready, indexesready)
	err := dr.WriteNWI4(w, cancell, nextready, indexesready)

	return err

}

func (dr *dagReader) WriteNWI4(w io.Writer, cancell context.CancelFunc, nextready chan struct{}, indexesready chan struct{}) error {
	linksparallel := make([]linkswithindexes, 0)
	enc, _ := reedsolomon.New(dr.or, dr.par)
	var written uint64
	written = 0
	countchecked := 0
	nbr := 0

	for _, n := range dr.nodesToExtr {
		for _, l := range n.Links() {
			if dr.toskip == true && len(linksparallel) == 0 && countchecked == 0 {
				fmt.Fprintf(os.Stdout, "1111111111 %s 111111111 \n", time.Now().String())
				if len(dr.retnext) < dr.or+dr.par {
					fmt.Fprintf(os.Stdout, "222222 %s 2222 \n", time.Now().String())
					topass := linkswithindexes{Link: l, Index: nbr % (dr.or + dr.par)}
					dr.retnext = append(dr.retnext, topass)
				}
				if len(dr.retnext) == dr.or+dr.par {
					fmt.Fprintf(os.Stdout, "3333333 %s 3333333 \n", time.Now().String())
					dr.Indexes = make([]int, 0)
					dr.toskip = false
					nextready <- struct{}{}
				}
			} else {
				tt := time.Now()
				if dr.stop == true {
					return nil
				}
				if len(dr.Indexes) != dr.or {
					select {
					case <-indexesready:
					}
				}
				fmt.Fprintf(os.Stdout, "4444444 wait until indexes are ready took %s 444444 \n", time.Since(tt))
				countchecked++
				tocheck := nbr % (dr.or + dr.par)
				if contains(dr.Indexes, tocheck) && len(linksparallel) < dr.or {
					topass := linkswithindexes{Link: l, Index: nbr % (dr.or + dr.par)}
					linksparallel = append(linksparallel, topass)
				}
				if len(linksparallel) == dr.or && countchecked == dr.or+dr.par {
					countchecked = 0
					stt := time.Now()
					//open channel with context
					doneChanR := make(chan nodeswithindexeswithtime, dr.or)
					// Create a new context with cancellation for this batch
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
					wrote := 0
					var mu sync.Mutex
					var wg sync.WaitGroup
					//start n+k gourotines and start retrieving parallel nodes
					wg.Add(dr.or)
					for _, topass := range linksparallel {
						go dr.worker(ctx, cancel, doneChanR, topass, &wrote, &mu, &wg)
					}
					//wait
					wg.Wait()
					fmt.Fprintf(os.Stdout, "5555555555 Retrieving the next set of chunks related to cids took : %s 55555555555 \n", time.Since(stt).String())
					//take from done channel
					close(doneChanR)
					shards := make([][]byte, dr.or+dr.par)
					reconstruct := 0
					for value := range doneChanR {
						// we will compare the indexes and see if they are from 0 to 2 but here we are trying just to write
						// Place the node's raw data into the correct index in shards
						shards[value.Index], _ = unixfs.ReadUnixFSNodeData(value.Node)
						if value.Index%(dr.or+dr.par) >= dr.or {
							reconstruct = 1
						}
						//dr.writeNodeDataBuffer(w)
					}
					if reconstruct == 1 {
						dr.recnostructtimes++
						start := time.Now()
						enc.Reconstruct(shards)
						end := time.Now()
						dr.timetakenDecode += end.Sub(start)
						st := time.Now()
						enc.Verify(shards)
						en := time.Now()
						dr.verificationTime += en.Sub(st)
					}
					dr.mu.Lock()
					for i, shard := range shards {
						if i < dr.or {
							if written+uint64(len(shard)) < dr.size {
								w.Write(shard)
								written += uint64(len(shard))
							} else {
								towrite := shard[0 : dr.size-written]
								w.Write(towrite)
								dr.stop = true
								cancell()
								dr.mu.Unlock()
								return nil
							}
						}
					}
					dr.mu.Unlock()
					linksparallel = make([]linkswithindexes, 0)
				}
			}
			nbr++

		}
	}
	dr.stop = true
	cancell()
	dr.ctx.Done()
	return nil
}

func (dr *dagReader) RetrieveAllSetNew4(w io.Writer, cancell context.CancelFunc, nextready chan struct{}, indexesready chan struct{}) {
	enc, _ := reedsolomon.New(dr.or, dr.par)
	//open channel with context
	doneChan := make(chan nodeswithindexeswithtime, dr.or)
	// Create a new context with cancellation for this batch
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	wrote := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	//start n+k gourotines and start retrieving parallel nodes
	wg.Add(dr.or)
	for i, link := range dr.retnext {
		topass := linkswithindexes{Link: link.Link, Index: i}
		go dr.worker(ctx, cancel, doneChan, topass, &wrote, &mu, &wg)
	}

	//wait
	wg.Wait()
	shards := make([][]byte, dr.or+dr.par)
	reconstruct := 0

	//take from done channel
	close(doneChan)
	for value := range doneChan {
		dr.Indexes = append(dr.Indexes, value.Index)
		dr.times = append(dr.times, value.t)
		shards[value.Index], _ = unixfs.ReadUnixFSNodeData(value.Node)
		if value.Index%(dr.or+dr.par) >= dr.or {
			reconstruct = 1
		}
	}
	dr.mu.Lock()
	dr.retnext = make([]linkswithindexes, 0)
	indexesready <- struct{}{}
	if reconstruct == 1 {
		dr.recnostructtimes++
		start := time.Now()
		enc.Reconstruct(shards)
		end := time.Now()
		dr.timetakenDecode += end.Sub(start)
		st := time.Now()
		enc.Verify(shards)
		en := time.Now()
		dr.verificationTime += en.Sub(st)
	}
	for i, shard := range shards {
		if i < dr.or {
			if dr.written+uint64(len(shard)) < dr.size {
				w.Write(shard)
				dr.written += uint64(len(shard))
			} else {
				towrite := shard[0 : dr.size-dr.written]
				w.Write(towrite)
				dr.stop = true
				dr.mu.Unlock()
				cancell()
				return
			}
		}
	}
	dr.mu.Unlock()
	// Send signal to the writing loop
	return
}

func (dr *dagReader) startTimerNew4(ctx context.Context, w io.Writer, cancell context.CancelFunc, nextready chan struct{}, indexesready chan struct{}) {
	ticker := time.NewTicker(time.Duration(dr.interval * float64(time.Second)))
	defer ticker.Stop()
	select {
	case <-nextready:
	}
	dr.RetrieveAllSetNew4(w, cancell, nextready, indexesready)
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Timer stopped")
			return
		case <-ticker.C:
			// Do the update by retrieving the next set of or + par chunks and update indexes with times
			// dont forget to mutex lock not to interfere
			ttt := time.Now()
			if dr.stop == true {
				close(nextready)
				close(indexesready)
				return
			}
			dr.toskip = true
			select {
			case <-nextready:
			}
			fmt.Fprintf(os.Stdout, "66666666 time taken timer waiting until next set is ready by the first thread is %s 66666666 \n", time.Since(ttt))
			fmt.Fprintf(os.Stdout, "---------------I WILLLL UPDATE THE INDEXES ----------------- \n")
			dr.RetrieveAllSetNew4(w, cancell, nextready, indexesready)
		}
	}
}

func (dr *dagReader) worker(ctx context.Context, cancel context.CancelFunc, doneChanR chan nodeswithindexeswithtime, nodepassed linkswithindexes, wrote *int, mu *sync.Mutex, wg *sync.WaitGroup) {
	st := time.Now()
	node, _ := nodepassed.Link.GetNode(ctx, dr.serv)
	t := time.Since(st)
	mu.Lock()
	defer mu.Unlock()
	select {
	case <-ctx.Done():
		// Context cancelled, goroutine terminates early
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Println("Timeout reached")
			dr.ctx.Done()
		}
		return
	default:
		*wrote++
		doneChanR <- nodeswithindexeswithtime{Node: node, Index: nodepassed.Index, t: t}
		if *wrote == dr.or {
			cancel()
		}
		wg.Done()
	}
}

////////////////////////////
/////////////////////////////////
//////////////////////////////////////////
//////////////////////////////////////////////////
/////////////////////////////////////////////////////////

func (dr *dagReader) WriteNWID5(w io.Writer) error {
	ctxx, cancell := context.WithCancel(context.Background())
	go dr.startTimerNew5(ctxx)
	//err := dr.WriteNWI5(w, cancell)
	err := dr.WriteNWIMany(w, cancell)
	return err

}

func (dr *dagReader) WriteNWI5(w io.Writer, cancell context.CancelFunc) error {
	fmt.Fprintf(os.Stdout, "Start the function %s  \n", time.Now().Format("15:04:05.000"))
	linksparallel := make([]linkswithindexes, 0)
	enc, _ := reedsolomon.New(dr.or, dr.par)
	var written uint64
	written = 0
	countchecked := 0
	nbr := 0
	sixnine := false
	var checkstime time.Duration
	var writetime time.Duration
	var reconstructiontime time.Duration
	var downloadtimesixsix time.Duration
	var downloadtimesixnine time.Duration
	var readchanneltime time.Duration
	sixsixtime := 0
	sixninetime := 0
	nbver := 0

	for _, n := range dr.nodesToExtr {
		for _, l := range n.Links() {
			st := time.Now()
			fmt.Fprintf(os.Stdout, "to skip is %t; length of linkparallel is : %d;countchecked is: %d; length of retnext is : %d;length of indexes is: %d;nbr is: %d  \n", dr.toskip, len(linksparallel), countchecked, len(dr.retnext), len(dr.Indexes), nbr)
			if dr.toskip == true && len(linksparallel) == 0 && countchecked == 0 {
				checkstime += time.Since(st)
				st1 := time.Now()
				if len(dr.retnext) < dr.or+dr.par {
					checkstime += time.Since(st1)
					topass := linkswithindexes{Link: l, Index: nbr % (dr.or + dr.par)}
					dr.retnext = append(dr.retnext, topass)
					//fmt.Fprintf(os.Stdout, "Filled 1 in the retnext %s  \n", time.Now().Format("15:04:05.000"))
				} else {
					checkstime += time.Since(st1)
				}
				st2 := time.Now()
				if len(dr.retnext) == dr.or+dr.par {
					checkstime += time.Since(st2)
					dr.Indexes = make([]int, 0)
					dr.toskip = false
					sixnine = true
					//fmt.Fprintf(os.Stdout, "Finish filling the retnext %s  \n", time.Now().Format("15:04:05.000"))
				}
			} else {
				countchecked++
				//fmt.Fprintf(os.Stdout, "Check if the index of the current cid is included in the indexes to fill links parallel %s  \n", time.Now().Format("15:04:05.000"))
				st3 := time.Now()
				tocheck := nbr % (dr.or + dr.par)
				for _, i := range dr.Indexes {
					fmt.Fprintf(os.Stdout, "Indexes contains : %d  \n", i)
				}
				if contains(dr.Indexes, tocheck) && len(linksparallel) < dr.or {
					checkstime += time.Since(st3)
					topass := linkswithindexes{Link: l, Index: nbr % (dr.or + dr.par)}
					linksparallel = append(linksparallel, topass)
					//fmt.Fprintf(os.Stdout, "It is in and filled %s  \n", time.Now().Format("15:04:05.000"))
				}
				st4 := time.Now()
				if len(linksparallel) == dr.or && countchecked == dr.or+dr.par {
					checkstime += time.Since(st4)
					//fmt.Fprintf(os.Stdout, "Finished Filling links parallel %s  \n", time.Now().Format("15:04:05.000"))
					countchecked = 0
					//open channel with context
					doneChanR := make(chan nodeswithindexes, dr.or)
					// Create a new context with cancellation for this batch
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
					wrote := 0
					//start n+k gourotines and start retrieving parallel nodes

					defer cancel() // Ensure context is cancelled when batch is done
					//start n+k gourotines and start retrieving parallel nodes
					worker := func(nodepassed linkswithindexes) {
						node, _ := nodepassed.Link.GetNode(ctx, dr.serv)
						dr.mu.Lock()
						defer dr.mu.Unlock()
						select {
						case <-ctx.Done():
							// Context cancelled, goroutine terminates early
							if ctx.Err() == context.DeadlineExceeded {
								fmt.Println("Timeout reached")
								dr.ctx.Done()
							}
							return
						default:
							wrote++
							doneChanR <- nodeswithindexes{Node: node, Index: nodepassed.Index}
							if wrote == dr.or {
								cancel()
							}
							dr.wg.Done()
						}
					}
					d := time.Now()
					dr.wg.Add(dr.or)
					//fmt.Fprintf(os.Stdout, "Start download the linksparallel %s  \n", time.Now().Format("15:04:05.000"))
					for _, link := range linksparallel {
						go worker(link)
					}

					//wait
					dr.wg.Wait()
					//take from done channel
					close(doneChanR)
					downloadtimesixsix += time.Since(d)
					sixsixtime++
					d1 := time.Now()
					shards := make([][]byte, dr.or+dr.par)
					reconstruct := 0
					//fmt.Fprintf(os.Stdout, "Finished downloading linksparallel and start reading from the channel links parallel %s  \n", time.Now().Format("15:04:05.000"))
					for value := range doneChanR {
						// we will compare the indexes and see if they are from 0 to 2 but here we are trying just to write
						// Place the node's raw data into the correct index in shards
						fmt.Fprintf(os.Stdout, "Index : %d  \n", value.Index)
						shards[value.Index], _ = unixfs.ReadUnixFSNodeData(value.Node)
						if value.Index >= dr.or {
							reconstruct = 1
						}
						//dr.writeNodeDataBuffer(w)
					}
					fmt.Fprintf(os.Stdout, "-------------------------- \n")
					readchanneltime += time.Since(d1)
					//fmt.Fprintf(os.Stdout, "Finished reading from channel linksparallel and start reconstruction and verification links parallel %s  \n", time.Now().Format("15:04:05.000"))
					if reconstruct == 1 {
						nbver++
						sss := time.Now()
						dr.recnostructtimes++
						start := time.Now()
						enc.Reconstruct(shards)
						end := time.Now()
						dr.timetakenDecode += end.Sub(start)
						st := time.Now()
						enc.Verify(shards)
						en := time.Now()
						dr.verificationTime += en.Sub(st)
						reconstructiontime += time.Since(sss)
					}
					//fmt.Fprintf(os.Stdout, "Finished reconstruction and verification and start of writing links parallel  %s  \n", time.Now().Format("15:04:05.000"))

					wr := time.Now()
					for i, shard := range shards {
						if i < dr.or {
							if written+uint64(len(shard)) <= dr.size {
								w.Write(shard)
								written += uint64(len(shard))
								writetime += time.Since(wr)
							} else {
								towrite := shard[0 : dr.size-written]
								w.Write(towrite)
								dr.stop = true
								cancell()
								writetime += time.Since(wr)
								fmt.Fprintf(os.Stdout, "New log download time six six is : %s  \n", downloadtimesixsix.String())
								fmt.Fprintf(os.Stdout, "New log download time six nine is : %s  \n", downloadtimesixnine.String())
								fmt.Fprintf(os.Stdout, "New log number of six six stripes is : %d  \n", sixsixtime)
								fmt.Fprintf(os.Stdout, "New log number of six nine stripes is : %d  \n", sixninetime)
								fmt.Fprintf(os.Stdout, "New log write time is : %s  \n", writetime.String())
								fmt.Fprintf(os.Stdout, "New log reconstruction and verification time is : %s  \n", checkstime.String())
								fmt.Fprintf(os.Stdout, "New log number of reconstructions is : %d  \n", nbver)
								fmt.Fprintf(os.Stdout, "New log read from channel time is : %s  \n", readchanneltime.String())
								return nil
							}
						}
					}
					//fmt.Fprintf(os.Stdout, "Finished writing links parallel %s  \n", time.Now().Format("15:04:05.000"))
					linksparallel = make([]linkswithindexes, 0)
				} else {
					checkstime += time.Since(st4)
				}
			}
			st5 := time.Now()
			if sixnine {
				checkstime += time.Since(st5)
				countchecked = 0
				//open channel with context
				doneChanR := make(chan nodeswithindexes, dr.or)
				// Create a new context with cancellation for this batch
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
				wrote1 := 0
				//start n+k gourotines and start retrieving parallel nodes

				defer cancel() // Ensure context is cancelled when batch is done
				//start n+k gourotines and start retrieving parallel nodes
				worker1 := func(nodepassed linkswithindexes) {
					node, _ := nodepassed.Link.GetNode(ctx, dr.serv)
					dr.mu.Lock()
					defer dr.mu.Unlock()
					select {
					case <-ctx.Done():
						// Context cancelled, goroutine terminates early
						if ctx.Err() == context.DeadlineExceeded {
							fmt.Println("Timeout reached")
							dr.ctx.Done()
						}
						return
					default:
						wrote1++
						doneChanR <- nodeswithindexes{Node: node, Index: nodepassed.Index}
						if wrote1 == dr.or {
							cancel()
						}
						dr.wg.Done()
					}
				}
				d2 := time.Now()
				dr.wg.Add(dr.or)
				//fmt.Fprintf(os.Stdout, "Start downloading retnext %s  \n", time.Now().Format("15:04:05.000"))
				for _, link := range dr.retnext {
					go worker1(link)
				}

				//wait
				dr.wg.Wait()
				//take from done channel
				close(doneChanR)
				downloadtimesixnine += time.Since(d2)
				sixninetime++
				d3 := time.Now()
				//fmt.Fprintf(os.Stdout, "Finished downloading retnext and start reading from channel retnext %s  \n", time.Now().Format("15:04:05.000"))
				shards := make([][]byte, dr.or+dr.par)
				reconstruct := 0
				for value := range doneChanR {
					// we will compare the indexes and see if they are from 0 to 2 but here we are trying just to write
					// Place the node's raw data into the correct index in shards
					fmt.Fprintf(os.Stdout, "Index : %d  \n", value.Index)
					dr.Indexes = append(dr.Indexes, value.Index)
					shards[value.Index], _ = unixfs.ReadUnixFSNodeData(value.Node)
					if value.Index >= dr.or {
						reconstruct = 1
					}
					//dr.writeNodeDataBuffer(w)
				}
				fmt.Fprintf(os.Stdout, "-------------------------- \n")
				readchanneltime += time.Since(d3)
				//fmt.Fprintf(os.Stdout, "Finished reading from channel and updating indexes and reading to shards nad start reconstruction and verification retnext %s  \n", time.Now().Format("15:04:05.000"))
				if reconstruct == 1 {
					nbver++
					sss1 := time.Now()
					dr.recnostructtimes++
					start := time.Now()
					enc.Reconstruct(shards)
					end := time.Now()
					dr.timetakenDecode += end.Sub(start)
					st := time.Now()
					enc.Verify(shards)
					en := time.Now()
					dr.verificationTime += en.Sub(st)
					reconstructiontime += time.Since(sss1)
				}
				//fmt.Fprintf(os.Stdout, "Finished reconstruction and verification and start writing retnext %s  \n", time.Now().Format("15:04:05.000"))
				wr1 := time.Now()
				for i, shard := range shards {
					if i < dr.or {
						if written+uint64(len(shard)) <= dr.size {
							w.Write(shard)
							written += uint64(len(shard))
							writetime += time.Since(wr1)
						} else {
							towrite := shard[0 : dr.size-written]
							w.Write(towrite)
							dr.stop = true
							cancell()
							writetime += time.Since(wr1)
							fmt.Fprintf(os.Stdout, "New log download time six six is : %s  \n", downloadtimesixsix.String())
							fmt.Fprintf(os.Stdout, "New log download time six nine is : %s  \n", downloadtimesixnine.String())
							fmt.Fprintf(os.Stdout, "New log number of six six stripes is : %d  \n", sixsixtime)
							fmt.Fprintf(os.Stdout, "New log number of six nine stripes is : %d  \n", sixninetime)
							fmt.Fprintf(os.Stdout, "New log write time is : %s  \n", writetime.String())
							fmt.Fprintf(os.Stdout, "New log reconstruction and verification time is : %s  \n", checkstime.String())
							fmt.Fprintf(os.Stdout, "New log number of reconstructions is : %d  \n", nbver)
							fmt.Fprintf(os.Stdout, "New log read from channel time is : %s  \n", readchanneltime.String())
							return nil
						}
					}
				}
				///	fmt.Fprintf(os.Stdout, "Finished writing retnext %s  \n", time.Now().Format("15:04:05.000"))
				linksparallel = make([]linkswithindexes, 0)
				dr.retnext = make([]linkswithindexes, 0)
				sixnine = false
			} else {
				checkstime += time.Since(st5)
			}
			nbr++
		}
	}
	dr.stop = true
	cancell()
	dr.ctx.Done()
	fmt.Fprintf(os.Stdout, "New log download time six six is : %s  \n", downloadtimesixsix.String())
	fmt.Fprintf(os.Stdout, "New log download time six nine is : %s  \n", downloadtimesixnine.String())
	fmt.Fprintf(os.Stdout, "New log number of six six stripes is : %d  \n", sixsixtime)
	fmt.Fprintf(os.Stdout, "New log number of six nine stripes is : %d  \n", sixninetime)
	fmt.Fprintf(os.Stdout, "New log write time is : %s  \n", writetime.String())
	fmt.Fprintf(os.Stdout, "New log reconstruction and verification time is : %s  \n", checkstime.String())
	fmt.Fprintf(os.Stdout, "New log number of reconstructions is : %d  \n", nbver)
	fmt.Fprintf(os.Stdout, "New log read from channel time is : %s  \n", readchanneltime.String())
	return nil
}

func (dr *dagReader) startTimerNew5(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(dr.interval * float64(time.Second)))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Timer stopped")
			return
		case <-ticker.C:
			// Do the update by retrieving the next set of or + par chunks and update indexes with times
			// dont forget to mutex lock not to interfere

			if dr.stop == true {
				return
			}
			dr.toskip = true

		}
	}
}

////////////////////////////////// With getmany implementation ////////////////////////////////////

func (dr *dagReader) WriteNWIMany(w io.Writer, cancell context.CancelFunc) error {
	fmt.Fprintf(os.Stdout, "Start the function %s  \n", time.Now().Format("15:04:05.000"))
	linksparallel := make([]linkswithindexes, 0)
	enc, _ := reedsolomon.New(dr.or, dr.par)
	var written uint64
	written = 0
	countchecked := 0
	nbr := 0
	sixnine := false
	var checkstime time.Duration
	var writetime time.Duration
	var reconstructiontime time.Duration
	var downloadtimesixsix time.Duration
	var downloadtimesixnine time.Duration
	sixsixtime := 0
	sixninetime := 0
	nbver := 0

	for _, n := range dr.nodesToExtr {
		for _, l := range n.Links() {
			st := time.Now()
			fmt.Fprintf(os.Stdout, "to skip is %t; length of linkparallel is : %d;countchecked is: %d; length of retnext is : %d;length of indexes is: %d;nbr is: %d  \n", dr.toskip, len(linksparallel), countchecked, len(dr.retnext), len(dr.Indexes), nbr)
			if dr.toskip == true && len(linksparallel) == 0 && countchecked == 0 {
				checkstime += time.Since(st)
				st1 := time.Now()
				if len(dr.retnext) < dr.or+dr.par {
					checkstime += time.Since(st1)
					topass := linkswithindexes{Link: l, Index: nbr % (dr.or + dr.par)}
					dr.retnext = append(dr.retnext, topass)
					//fmt.Fprintf(os.Stdout, "Filled 1 in the retnext %s  \n", time.Now().Format("15:04:05.000"))
				} else {
					checkstime += time.Since(st1)
				}
				st2 := time.Now()
				if len(dr.retnext) == dr.or+dr.par {
					checkstime += time.Since(st2)
					dr.Indexes = make([]int, 0)
					dr.toskip = false
					sixnine = true
					//fmt.Fprintf(os.Stdout, "Finish filling the retnext %s  \n", time.Now().Format("15:04:05.000"))
				}
			} else {
				countchecked++
				//fmt.Fprintf(os.Stdout, "Check if the index of the current cid is included in the indexes to fill links parallel %s  \n", time.Now().Format("15:04:05.000"))
				st3 := time.Now()
				tocheck := nbr % (dr.or + dr.par)
				for _, i := range dr.Indexes {
					fmt.Fprintf(os.Stdout, "Indexes contains : %d  \n", i)
				}
				if contains(dr.Indexes, tocheck) && len(linksparallel) < dr.or {
					checkstime += time.Since(st3)
					topass := linkswithindexes{Link: l, Index: nbr % (dr.or + dr.par)}
					linksparallel = append(linksparallel, topass)
					//fmt.Fprintf(os.Stdout, "It is in and filled %s  \n", time.Now().Format("15:04:05.000"))
				}
				st4 := time.Now()
				if len(linksparallel) == dr.or && countchecked == dr.or+dr.par {
					checkstime += time.Since(st4)
					//fmt.Fprintf(os.Stdout, "Finished Filling links parallel %s  \n", time.Now().Format("15:04:05.000"))
					countchecked = 0
					//open channel with context
					// Create a new context with cancellation for this batch
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
					//start n+k gourotines and start retrieving parallel nodes
					d := time.Now()
					dr.wg.Add(dr.or)
					togetmany := make([]cid.Cid, 0)
					//fmt.Fprintf(os.Stdout, "Start download the linksparallel %s  \n", time.Now().Format("15:04:05.000"))
					// Create a map of CID -> Index from linksparallel
					wrote :=0
					cidIndexMap := make(map[cid.Cid][]int)
for _, ci := range linksparallel {
    togetmany = append(togetmany, ci.Link.Cid)
    cidIndexMap[ci.Link.Cid] = append(cidIndexMap[ci.Link.Cid], ci.Index)
}

// Launch GetMany
chann := dr.serv.GetMany(ctx, togetmany)
shards := make([][]byte, dr.or+dr.par)
reconstruct := 0

// Read from channel
for value := range chann {

    indexes, ok := cidIndexMap[value.Node.Cid()]
    if !ok {
        // Should not happen unless GetMany returns unexpected CIDs
        continue
    }

    // One CID might correspond to multiple indexes
    for _, idx := range indexes {
		 wrote++
        fmt.Fprintf(os.Stdout, "66 index : %d  \n", idx)
        shards[idx], _ = unixfs.ReadUnixFSNodeData(value.Node)
        if idx >= dr.or {
            reconstruct = 1
        }
        dr.wg.Done()
		 if wrote >= dr.or {
			 break
		 }
    }

    if wrote >= dr.or {
        cancel()
        break
    }
}

// Wait
dr.wg.Wait()
					//time.Sleep(100 * time.Millisecond)
					downloadtimesixsix += time.Since(d)
					sixsixtime++
					//fmt.Fprintf(os.Stdout, "Finished reading from channel linksparallel and start reconstruction and verification links parallel %s  \n", time.Now().Format("15:04:05.000"))
					if reconstruct == 1 {
						nbver++
						sss := time.Now()
						dr.recnostructtimes++
						start := time.Now()
						enc.Reconstruct(shards)
						end := time.Now()
						dr.timetakenDecode += end.Sub(start)
						st := time.Now()
						enc.Verify(shards)
						en := time.Now()
						dr.verificationTime += en.Sub(st)
						reconstructiontime += time.Since(sss)
					}
					//fmt.Fprintf(os.Stdout, "Finished reconstruction and verification and start of writing links parallel  %s  \n", time.Now().Format("15:04:05.000"))


					for i, shard := range shards {
						if i < dr.or {
							if written+uint64(len(shard)) <= dr.size {
								wr := time.Now()
								w.Write(shard)
								written += uint64(len(shard))
								writetime += time.Since(wr)
							} else {
								wr := time.Now()
								towrite := shard[0 : dr.size-written]
								w.Write(towrite)
								dr.stop = true
								cancell()
								writetime += time.Since(wr)
								fmt.Fprintf(os.Stdout, "New log download time six six is : %s  \n", downloadtimesixsix.String())
								fmt.Fprintf(os.Stdout, "New log download time six nine is : %s  \n", downloadtimesixnine.String())
								fmt.Fprintf(os.Stdout, "New log number of six six stripes is : %d  \n", sixsixtime)
								fmt.Fprintf(os.Stdout, "New log number of six nine stripes is : %d  \n", sixninetime)
								fmt.Fprintf(os.Stdout, "New log write time is : %s  \n", writetime.String())
								fmt.Fprintf(os.Stdout, "New log checks time is : %s  \n", checkstime.String())
								fmt.Fprintf(os.Stdout, "New log reconstruction and verification time is : %s  \n", reconstructiontime.String())
								fmt.Fprintf(os.Stdout, "New log number of reconstructions is : %d  \n", nbver)
								return nil
							}
						}
					}
					//fmt.Fprintf(os.Stdout, "Finished writing links parallel %s  \n", time.Now().Format("15:04:05.000"))
					linksparallel = make([]linkswithindexes, 0)
				} else {
					checkstime += time.Since(st4)
				}
			}
			st5 := time.Now()
			if sixnine {
				wrote := 0
				checkstime += time.Since(st5)
				countchecked = 0
				//open channel with context
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
				//start n+k gourotines and start retrieving parallel nodes
				dr.wg.Add(dr.or)
				togetmany := make([]cid.Cid, 0)
				d := time.Now()
				//fmt.Fprintf(os.Stdout, "Start download the linksparallel %s  \n", time.Now().Format("15:04:05.000"))
				// Create a map of CID -> Index from linksparallel
cidIndexMap := make(map[cid.Cid][]int)
for _, ci := range dr.retnext {
    togetmany = append(togetmany, ci.Link.Cid)
    cidIndexMap[ci.Link.Cid] = append(cidIndexMap[ci.Link.Cid], ci.Index)
}

// Launch GetMany
chann := dr.serv.GetMany(ctx, togetmany)
shards := make([][]byte, dr.or+dr.par)
reconstruct := 0

// Read from channel
for value := range chann {

    indexes, ok := cidIndexMap[value.Node.Cid()]
    if !ok {
        // Should not happen unless GetMany returns unexpected CIDs
        continue
    }
	k:=0
    // One CID might correspond to multiple indexes
    for _, idx := range indexes {
		wrote++
		k++
        fmt.Fprintf(os.Stdout, "69 index : %d with k is %d  \n", idx,k)
        dr.Indexes = append(dr.Indexes, idx)
        shards[idx], _ = unixfs.ReadUnixFSNodeData(value.Node)
        if idx >= dr.or {
            reconstruct = 1
        }
        dr.wg.Done()
		 if wrote >= dr.or {
			 fmt.Fprintf(os.Stdout, "I will break  \n")
			 break
		 }
    }

    if wrote >= dr.or {
        cancel()
        break
    }
}

// Wait
dr.wg.Wait()
				//time.Sleep(100 * time.Millisecond)
				downloadtimesixnine += time.Since(d)
				sixninetime++
				//fmt.Fprintf(os.Stdout, "Finished reading from channel and updating indexes and reading to shards nad start reconstruction and verification retnext %s  \n", time.Now().Format("15:04:05.000"))
				if reconstruct == 1 {
					nbver++
					sss1 := time.Now()
					dr.recnostructtimes++
					start := time.Now()
					enc.Reconstruct(shards)
					end := time.Now()
					dr.timetakenDecode += end.Sub(start)
					st := time.Now()
					enc.Verify(shards)
					en := time.Now()
					dr.verificationTime += en.Sub(st)
					reconstructiontime += time.Since(sss1)
				}
				//fmt.Fprintf(os.Stdout, "Finished reconstruction and verification and start writing retnext %s  \n", time.Now().Format("15:04:05.000"))
				for i, shard := range shards {
					if i < dr.or {
						if written+uint64(len(shard)) <= dr.size {
							wr1 := time.Now()
							w.Write(shard)
							written += uint64(len(shard))
							writetime += time.Since(wr1)
						} else {
							wr1 := time.Now()
							towrite := shard[0 : dr.size-written]
							w.Write(towrite)
							dr.stop = true
							cancell()
							writetime += time.Since(wr1)
							fmt.Fprintf(os.Stdout, "New log download time six six is : %s  \n", downloadtimesixsix.String())
							fmt.Fprintf(os.Stdout, "New log download time six nine is : %s  \n", downloadtimesixnine.String())
							fmt.Fprintf(os.Stdout, "New log number of six six stripes is : %d  \n", sixsixtime)
							fmt.Fprintf(os.Stdout, "New log number of six nine stripes is : %d  \n", sixninetime)
							fmt.Fprintf(os.Stdout, "New log write time is : %s  \n", writetime.String())
							fmt.Fprintf(os.Stdout, "New log checks time is : %s  \n", checkstime.String())
							fmt.Fprintf(os.Stdout, "New log reconstruction and verification time is : %s  \n", reconstructiontime.String())
							fmt.Fprintf(os.Stdout, "New log number of reconstructions is : %d  \n", nbver)
							return nil
						}
					}
				}
				///	fmt.Fprintf(os.Stdout, "Finished writing retnext %s  \n", time.Now().Format("15:04:05.000"))
				linksparallel = make([]linkswithindexes, 0)
				dr.retnext = make([]linkswithindexes, 0)
				sixnine = false
			} else {
				checkstime += time.Since(st5)
			}
			nbr++
		}
	}
	dr.stop = true
	cancell()
	dr.ctx.Done()
	fmt.Fprintf(os.Stdout, "New log download time six six is : %s  \n", downloadtimesixsix.String())
	fmt.Fprintf(os.Stdout, "New log download time six nine is : %s  \n", downloadtimesixnine.String())
	fmt.Fprintf(os.Stdout, "New log number of six six stripes is : %d  \n", sixsixtime)
	fmt.Fprintf(os.Stdout, "New log number of six nine stripes is : %d  \n", sixninetime)
	fmt.Fprintf(os.Stdout, "New log write time is : %s  \n", writetime.String())
	fmt.Fprintf(os.Stdout, "New log checks time is : %s  \n", checkstime.String())
	fmt.Fprintf(os.Stdout, "New log reconstruction and verification time is : %s  \n", reconstructiontime.String())
	fmt.Fprintf(os.Stdout, "New log number of reconstructions is : %d  \n", nbver)
	return nil
}

// //////////////////// Streaming each set of shards before writing them to disk before movving to the next set of shards Contigouos data layout /////////////////////
func (dr *dagReader) WriteCont(w io.Writer) (err error) {
	retnext := make([][]cid.Cid, dr.or+dr.par)
	//var writetime time.Duration
	//var downloadtime time.Duration
	fmt.Fprintf(os.Stdout, "111111111111111111111  \n")
	//shardswritten := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	i := 0
	//countoflinks := 0
	var datawrittentofile uint64
	//filesize := dr.size - uint64(dr.par)*dr.chunksize
	for _, n := range dr.nodesToExtr {
		for _, l := range n.Links() {
			if len(retnext[i]) < 400 {
				retnext[i] = append(retnext[i], l.Cid)
				if len(retnext[i]) == 400 && i == dr.or+dr.par-1 {
					fmt.Fprintf(os.Stdout, "5555555555555555555  \n")
					wg.Add(dr.or)
					shards := make([][]byte, dr.or+dr.par)
					for j := range retnext {
   go func(j int) {
    inputCIDs := retnext[j] // exactly 400 CIDs per shard

    // 1. Map each CID to all positions it occurs in (to preserve duplicates)
    posMap := make(map[cid.Cid][]int)
    for idx, c := range inputCIDs {
        posMap[c] = append(posMap[c], idx)
    }

    // 2. Allocate slice for blocks in original order
    blocks := make([][]byte, len(inputCIDs))

    // 3. Fetch blocks from GetMany
    ch := dr.serv.GetMany(dr.ctx, inputCIDs)

    for value := range ch {
        if value == nil || value.Node == nil {
            panic("GetMany returned nil Node for a CID") // safety check
        }

        cidVal := value.Node.Cid()
        indexes, ok := posMap[cidVal]
        if !ok {
            panic(fmt.Sprintf("Unexpected CID returned by GetMany: %s", cidVal))
        }

        data, err := unixfs.ReadUnixFSNodeData(value.Node)
        if err != nil {
            panic(fmt.Sprintf("Failed to read block data for CID %s: %v", cidVal, err))
        }

        // Fill all positions corresponding to duplicates
        for _, idx := range indexes {
            blocks[idx] = data
        }
    }

    // 4. Check that all blocks were filled
    for i, blk := range blocks {
        if blk == nil {
            panic(fmt.Sprintf("Block at index %d was not filled", i))
        }
    }

    // 5. Rebuild the shard in original order, including duplicates
    datastreamed := make([]byte, 0, len(blocks)*len(blocks[0]))
    for _, blk := range blocks {
        datastreamed = append(datastreamed, blk...)
    }

    // 6. Safely write the shard to the shared slice
    mu.Lock()
    shards[j] = datastreamed
    mu.Unlock()

    // 7. Mark shard as done
    wg.Done()
}(j)

}
					wg.Wait()
					fmt.Fprintf(os.Stdout, "666666666666666666666666  \n")
					//contain any parity ? reconstruct if yes
					//enc, _ := reedsolomon.New(dr.or, dr.par)
					//enc.Reconstruct(shards)
					//write data\
					for c, shard := range shards {
						fmt.Fprintf(os.Stdout, "10000001000000100000 shard number %d  \n", c)
						if shard != nil {
							if datawrittentofile+uint64(len(shard)) < dr.size {
								fmt.Fprintf(os.Stdout, "777777777777777777777  \n")
								w.Write(shard)
								datawrittentofile += uint64(len(shard))
							} else {
								if datawrittentofile+uint64(len(shard)) == dr.size {
									fmt.Fprintf(os.Stdout, "the sammmeeeeeeeee  \n")
									w.Write(shard)
									return nil
								} else {
									fmt.Fprintf(os.Stdout, "lessssssss thannnnnnnnnn  \n")
									towrite := shard[0 : dr.size-datawrittentofile]
									w.Write(towrite)
									return nil
								}
							}
						} else {
							fmt.Fprintf(os.Stdout, "skipppeeeeeeeedddddddddddddddddd  \n")
						}
					}
					retnext = make([][]cid.Cid, dr.or+dr.par)
					i = 0
					shardswritten = 0
				}
			} else {
				i++
				retnext[i] = append(retnext[i], l.Cid)
				fmt.Fprintf(os.Stdout, "44444444444444444444  \n")
			}
		}
	}
	//fmt.Fprintf(os.Stdout, "New log write time is : %s  \n", writetime.String())
	//fmt.Fprintf(os.Stdout, "New log download time is : %s  \n", downloadtime.String())
	return nil
}
