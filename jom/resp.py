class Resp:
    def __init__(self, *, message=None, fileobj=None, keyboard=None,
                 preview=False, markdown=False, inline=None):
        self.message = message
        self.fileobj = fileobj
        self.keyboard = keyboard
        self.preview = preview
        self.markdown = markdown
        self.inline = inline
