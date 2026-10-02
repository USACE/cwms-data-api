const REGEX_META_CHARACTERS = /[.+?^${}()|[\]\\]/g;

export function toUsernameRegex(search) {
  return search.replace(REGEX_META_CHARACTERS, "\\$&").replace(/\*/g, ".*");
}
