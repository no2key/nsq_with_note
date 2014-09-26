package util


// 给定string数组和一个string，线性查找数组中是否存在该string，不存在则append进去。
func StringAdd(s []string, a string) []string {
	for _, existing := range s {
		if a == existing {
			return s
		}
	}
	return append(s, a)

}
// O(n^2)复杂度的两个字符串数组union
func StringUnion(s []string, a []string) []string {
	for _, entry := range a {
		found := false
		for _, existing := range s {
			if entry == existing {
				found = true
				break
			}
		}
		if !found {
			s = append(s, entry)
		}
	}
	return s
}
