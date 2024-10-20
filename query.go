package columbus

// Query represents the sql query used by Mapper (or Mapper.Rows etc.)
// it should exclude the 'SELECT cols' - as Mapper already knows the columns to be mapped
type Query string

// AddClause is a sql clause that can be added when using Mapper.Rows, Mapper.FirstRow or Mapper.ExactlyOneRow
type AddClause string
