use anyhow::Result;

fn main() -> Result<()> {
    /*let foo: youtube_dl_wrapper::ytdl::ItemId =
        serde_json::from_str("\"Z-LuGrTir2E\"")?;
    eprintln!("{:?} = {}", &foo, serde_json::to_string_pretty(&foo)?);*/
    youtube_dl_wrapper::main()
}
