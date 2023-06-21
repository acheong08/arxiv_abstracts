package main

import (
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

func parquetToStruct(data interface{}) *Document {
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

func convertFile(filename string) error {
	fr, err := local.NewLocalFileReader("abstracts/" + filename)
	if err != nil {
		return err
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, jsonSchema, 4)
	if err != nil {
		return err
	}
	defer pr.ReadStop()

	numRows := int(pr.GetNumRows())

	// Create the output file
	outputFile, err := os.Create("converted/" + filename + ".gob")
	if err != nil {
		return err
	}
	defer outputFile.Close()

	enc := gob.NewEncoder(outputFile)

	batchSize := 1000 // Adjust the batch size as per your memory requirements

	for i := 0; i < numRows; i += batchSize {
		print(".")
		// Read a batch of rows
		batchSizeActual := batchSize
		if i+batchSize > numRows {
			batchSizeActual = numRows - i
		}

		res, err := pr.ReadByNumber(batchSizeActual)
		if err != nil {
			return err
		}

		// Convert and encode the documents in the batch
		documents := make([]Document, batchSizeActual)
		for j := 0; j < batchSizeActual; j++ {
			documents[j] = *parquetToStruct(res[j])
		}

		// Encode and write the documents to the output file
		err = enc.Encode(documents)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	// List all files in abstracts/
	files, err := os.ReadDir("abstracts/")
	if err != nil {
		log.Fatal("Error reading directory:", err)
	}

	for _, file := range files {
		log.Println("Converting", file.Name())
		err := convertFile(file.Name())
		if err != nil {
			log.Println("Conversion error:", err)
		}
	}
}
