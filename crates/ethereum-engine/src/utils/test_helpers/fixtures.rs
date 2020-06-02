use super::utils::TestAccount;
use mockito::Matcher;
use once_cell::sync::Lazy;

pub static ALICE: Lazy<TestAccount> = Lazy::new(|| {
    TestAccount::new(
        "1".to_string(),
        "3cdb3d9e1b74692bb1e3bb5fc81938151ca64b02",
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    )
});

pub static BOB: Lazy<TestAccount> = Lazy::new(|| {
    TestAccount::new(
        "0".to_string(),
        "9b925641c5ef3fd86f63bff2da55a0deeafd1263",
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    )
});

pub static SETTLEMENT_API: Lazy<Matcher> =
    Lazy::new(|| Matcher::Regex(r"^/accounts/\d*/settlements$".to_string()));

pub static MESSAGES_API: Lazy<Matcher> =
    Lazy::new(|| Matcher::Regex(r"^/accounts/\d*/messages$".to_string()));
