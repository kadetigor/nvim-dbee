package handler

import (
	"fmt"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"

	"github.com/kndndrj/nvim-dbee/dbee/core"
)

var _ core.Formatter = (*Table)(nil)

// display-only cap for a single cell; exports (csv/json/yank) are unaffected
const maxCellDisplayLength = 100

type Table struct{}

// displayValue renders a cell for the result table: multiline values (e.g.
// pretty-printed json) are flattened to one line and long values are cut off
// with an ellipsis. Store/yank paths don't go through here, so they still get
// full values.
func displayValue(val any) any {
	s := fmt.Sprint(val)
	if !strings.ContainsAny(s, "\n\t") && len(s) <= maxCellDisplayLength {
		return val
	}

	s = strings.Join(strings.Fields(s), " ")
	if runes := []rune(s); len(runes) > maxCellDisplayLength {
		s = string(runes[:maxCellDisplayLength-1]) + "…"
	}
	return s
}

func newTable() *Table {
	return &Table{}
}

func (tf *Table) Format(header core.Header, rows []core.Row, opts *core.FormatterOptions) ([]byte, error) {
	tableHeaders := []any{""}
	for _, k := range header {
		tableHeaders = append(tableHeaders, k)
	}
	index := opts.ChunkStart

	var tableRows []table.Row
	for _, row := range rows {
		indexedRow := make([]any, 0, len(row)+1)
		indexedRow = append(indexedRow, index+1)
		for _, val := range row {
			indexedRow = append(indexedRow, displayValue(val))
		}
		tableRows = append(tableRows, table.Row(indexedRow))
		index += 1
	}

	t := table.NewWriter()
	t.AppendHeader(table.Row(tableHeaders))
	t.AppendRows(tableRows)
	t.AppendSeparator()
	t.SetStyle(table.StyleLight)
	t.Style().Format = table.FormatOptions{
		Footer: text.FormatDefault,
		Header: text.FormatDefault,
		Row:    text.FormatDefault,
	}
	t.Style().Options.DrawBorder = false
	t.SuppressTrailingSpaces()
	render := t.Render()

	return []byte(render), nil
}
