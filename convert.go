package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type Document struct {
	Text      string
	Embedding []float32
	Doi       string
}

const jsonSchema = `{
  "Tag": "name=parquet_go_root, repetitiontype=REQUIRED",
  "Fields": [
    {
      "Tag": "name=abstract, inname=Abstract, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"
    },
    {
      "Tag": "name=embeddings, inname=Embeddings, type=LIST, repetitiontype=OPTIONAL",
      "Fields": [
				{
					"Tag": "name=item, type=FLOAT, repetitiontype=OPTIONAL"
				}
      ]
    },
    {
      "Tag": "name=doi, inname=Doi, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"
    }
  ]
}
`

func Convert(data interface{}) *Document {
	// Convert res to the actual result type
	result := data.(struct {
		Abstract   *string
		Embeddings *struct {
			List []struct {
				Item *float32
			}
		}
		Doi *string
	})

	doc := &Document{
		Text: *result.Abstract,
		Doi:  *result.Doi,
	}
	embeddings := make([]float32, len(result.Embeddings.List))
	for i, emb := range result.Embeddings.List {
		embeddings[i] = *emb.Item
	}
	doc.Embedding = embeddings
	return doc
}

func main() {
	///read
	fr, err := local.NewLocalFileReader("abstracts/abstracts_1.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, jsonSchema, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	num_rows := int(pr.GetNumRows())

	res, err := pr.ReadByNumber(num_rows)
	if err != nil {
		log.Println("Can't read", err)
		return
	}
	pr.ReadStop()
	fr.Close()

	documents := make([]Document, num_rows)

	for i := 0; i < num_rows-1; i++ {
		documents[i] = *Convert(res[num_rows])
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(documents)
	if err != nil {
		log.Println("encode error:", err)
	}
	err = os.WriteFile("converted/abstracts_1.gob", buf.Bytes(), 0644)
	if err != nil {
		log.Println("write error:", err)
	}

}
