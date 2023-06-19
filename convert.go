package main

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type Document struct {
	Abstract   *string
	Embeddings *[]float32
	Doi        *string
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
		Abstract: result.Abstract,
		Doi:      result.Doi,
	}
	embeddings := make([]float32, len(result.Embeddings.List))
	for i, emb := range result.Embeddings.List {
		embeddings[i] = *emb.Item
	}
	doc.Embeddings = &embeddings
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

	res, err := pr.ReadByNumber(int(pr.GetNumRows()))
	if err != nil {
		log.Println("Can't read", err)
		return
	}

	doc := Convert(res[int(pr.GetNumRows())-1])
	log.Println(*doc.Abstract)
	log.Println(*doc.Doi)
	log.Println(*doc.Embeddings)

	pr.ReadStop()
	fr.Close()
}
