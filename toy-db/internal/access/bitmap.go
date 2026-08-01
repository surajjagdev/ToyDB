package access

// Returns how many bytes to save for the nullbitmap
func GetBitMapSizeInBytes(columnCount int) int {
	if columnCount == 0 {
		return 0
	}

	// align to byte math
	return (columnCount + 7) / 8
}

// Get the byteIndex and bitIndex
func getByteInfo(colIndex int) (int, int) {
	// each byte hold 8 cols. divide by 8 to find the correct byte
	// the actual bit in byte

	byteIndex := colIndex / 8
	bitIndex := colIndex % 8

	return byteIndex, bitIndex
}

// Sets the bit in bitmap
func SetBitInBitMap(bitMap []byte, colIndex int) {
	byteIndex, bitIndex := getByteInfo(colIndex)

	// Turn on bit for the column
	bitMap[byteIndex] |= (1 << bitIndex)
}

// Unsets the bit in bitmap
func UnsetBitInBitMap(bitMap []byte, colIndex int) {
	byteIndex, bitIndex := getByteInfo(colIndex)

	// Turn off bit for the column
	bitMap[byteIndex] &= ^(1 << bitIndex)
}

// Check if bit is set
func IsBitSet(bitMap []byte, colIndex int) bool {
	if len(bitMap) == 0 {
		return false
	}

	byteIndex, bitIndex := getByteInfo(colIndex)

	return (bitMap[byteIndex] & (1 << bitIndex)) != 0
}
