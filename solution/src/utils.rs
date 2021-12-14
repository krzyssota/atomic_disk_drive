pub mod utils {
    use hmac::{Hmac, Mac, NewMac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;
    pub(crate) fn verify_hmac_tag(tag: &[u8], message: &[u8], hmac_key: &[u8]) -> bool {
        let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
        mac.update(message);
        mac.verify(tag).is_ok()
    }

    pub(crate) fn calculate_hmac_tag(message: &[u8], hmac_key: &[u8]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
        mac.update(message);
        let tag = mac.finalize().into_bytes();
        tag.into()
    }
}

